# Architecture Deep Dive — Redis Clone

This document explains how the project was built, **why** each piece was added,
**what OS concept** it represents, and **exactly how the code works** so you can
explain every line tomorrow.

---

## The Big Picture — How It All Fits Together

```
Client (telnet / our InteractiveClient)
        │  TCP socket on port 6379
        ▼
┌──────────────────────────────────────────────────────────┐
│  RedisServer.java                                        │
│  - Opens ServerSocket(6379)                              │
│  - accept() loop → wraps socket in ConnectionTask        │
│  - Submits to ConnectionPool                             │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│  ConnectionPool.java  (4 worker threads, queue cap=8)    │
│  - Each worker picks a ConnectionTask from the queue     │
│  - Creates a RedisClientHandler and calls run()          │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│  RedisClientHandler.java                                 │
│  - Reads bytes from socket → RespParser decodes command  │
│  - Wraps command in a CompletableFuture<CommandResult>   │
│  - Submits to CommandRouter                              │
│  - Blocks on future.join() until result is ready         │
│  - RespWriter encodes result → writes bytes to socket    │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│  CommandRouter.java                                      │
│  - Looks at the command verb (SET / LPUSH / HGET ...)    │
│  - Routes to the correct DataTypePartition               │
│  - Shared Semaphore(8) caps total concurrent DB ops      │
└────┬──────┬──────┬──────┬──────┬────────────────────────┘
     │      │      │      │      │
   STRING LIST   HASH   SET   ZSET
   4 wkrs 2 wkrs 2 wkrs 2 wkrs 2 wkrs
     │
     ▼
┌──────────────────────────────────────────────────────────┐
│  DataTypePartition.java                                  │
│  - Each worker drains its own BoundedTaskQueue           │
│  - Acquires dbPermits semaphore before running task      │
│  - task.run() → CommandHandler → RedisDatabase → store   │
│  - future.complete(result)                               │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│  RedisDatabase.java  (facade)                            │
│  - Delegates to StringStore / ListStore / HashStore ...  │
│  - After write: calls AofLogger.append(rawBytes)         │
└──────────────────────────┬───────────────────────────────┘
                           │
                     redis.aof  (disk)
```

---

## Step 1 — Start With the Problem

**The goal:** build a server that can store data in memory and answer commands from multiple clients simultaneously, just like Redis.

The first question you have to answer is:
> *What do we need before we can even accept a connection?*

Answer: a **TCP socket**, a **protocol** to decode what the client sends, and a **data store** to hold the values.

So the build order was:

```
1. Data store       (RedisDatabase + per-type stores)
2. Protocol         (RespParser + RespWriter)
3. Command logic    (CommandHandler + sub-handlers)
4. TCP server       (RedisServer)
5. Persistence      (AofLogger)
6. Concurrency      (ConnectionPool → CommandRouter → DataTypePartition)
7. Monitoring       (RedisPoolMonitor)
```

---

## Step 2 — The Protocol (RESP)

**File:** `src/com/redis/protocol/RespParser.java`, `RespWriter.java`

**Why we need it:** TCP is a raw byte stream. We need a convention to know where one command ends and the next begins. Redis uses the **RESP** (Redis Serialization Protocol).

### How RESP works

Every message starts with a type prefix character:

```
+   Simple string    → +OK\r\n
-   Error            → -ERR message\r\n
:   Integer          → :42\r\n
$   Bulk string      → $5\r\nMohan\r\n       (length first, then data)
*   Array            → *3\r\n...             (count first, then N elements)
```

A command like `SET name Mohan` arrives as:
```
*3\r\n        ← array of 3 parts
$3\r\n        ← part 1 is 3 bytes long
SET\r\n       ← the actual bytes
$4\r\n        ← part 2 is 4 bytes long
name\r\n
$5\r\n        ← part 3 is 5 bytes long
Mohan\r\n
```

**RespParser** reads this byte by byte from the socket InputStream and builds a `RespCommand` (which is just a `String[]` array like `["SET","name","Mohan"]`).

**RespWriter** does the reverse — takes a `CommandResult` and writes the RESP bytes back to the socket.

### Key code in RespParser

```java
// Reads the first byte to know the type
int typeByte = inputStream.read();
switch ((char) typeByte) {
    case '*' -> readArray();    // command is an array
    case '$' -> readBulkString();
    case '+' -> readSimpleString();
    // ...
}
```

---

## Step 3 — The Database Layer

**Files:** `RedisDatabase.java`, `db/store/StringStore.java`, `ListStore.java`, `HashStore.java`, `SetStore.java`, `SortedSetStore.java`, `ExpirationManager.java`

### Why a Facade?

`RedisDatabase` is a **facade** — it doesn't store any data itself. It just holds one instance of each store and routes calls to the right one.

```java
// RedisDatabase.java
private final StringStore    strings  = new StringStore();
private final ListStore      lists    = new ListStore(expirations);
private final HashStore      hashes   = new HashStore(expirations);
private final SetStore       sets     = new SetStore(expirations);
private final SortedSetStore zsets    = new SortedSetStore(expirations);
```

**Why five separate stores?** Because each data type uses a different Java data structure:

| Redis Type | Java Structure | Why |
|-----------|----------------|-----|
| String | `ConcurrentHashMap<String, RedisEntry>` | O(1) get/set, thread-safe |
| List | `LinkedList<String>` per key | O(1) head/tail insert for LPUSH/RPUSH |
| Hash | `HashMap<String,String>` per key | O(1) field lookup |
| Set | `HashSet<String>` per key | O(1) add/contains, automatic dedup |
| Sorted Set | custom dual-index structure | O(log n) score-ordered range queries |

### How TTL/Expiry works

`ExpirationManager` holds a map of `key → expiry timestamp (milliseconds)`.

```java
// When you do: SET name Mohan EX 60
long expiryAt = System.currentTimeMillis() + 60_000;
expirations.setExpirationMillis("name", expiryAt);
```

**Lazy expiry** — when you do `GET name`, the store checks:
```java
if (System.currentTimeMillis() > expiryTimestamp) {
    // key has expired — delete it and return null
}
```

There is also a **background sweeper** in `RedisServer` that runs every second:
```java
expirationScheduler.scheduleAtFixedRate(
    database::removeExpiredKeys, 1, 1, TimeUnit.SECONDS);
```
This cleans up keys that nobody ever reads again (active expiry).

---

## Step 4 — Command Handling

**Files:** `CommandHandler.java`, `db/handler/StringCommandHandler.java`, `ListCommandHandler.java`, etc.

### Why sub-handlers?

Originally all commands could have been in one giant `switch`. But that file would be thousands of lines long and impossible to maintain. So `CommandHandler` is a **dispatcher** — it routes by verb to a typed handler:

```java
// CommandHandler.java
return switch (operation) {
    case "SET","GET","DEL","INCR",...  -> stringHandler.handle(operation, command);
    case "LPUSH","RPUSH","LPOP",...    -> listHandler.handle(operation, command);
    case "HSET","HGET","HDEL",...      -> hashHandler.handle(operation, command);
    // ...
};
```

Each sub-handler (e.g. `StringCommandHandler`) parses the specific arguments for that command and calls the right method on `RedisDatabase`.

### CommandResult

Every command returns a `CommandResult`. This is a **value object** — it carries the type and the value:

```java
// Factory methods
CommandResult.simpleString("OK")     // → +OK\r\n
CommandResult.bulkString("Mohan")    // → $5\r\nMohan\r\n
CommandResult.integer(42)            // → :42\r\n
CommandResult.array(list)            // → *N\r\n...
CommandResult.nil()                  // → $-1\r\n
CommandResult.error("ERR msg")       // → -ERR msg\r\n
```

`RespWriter` reads the type and writes the correct RESP bytes.

---

## Step 5 — AOF Persistence

**File:** `src/com/redis/server/AofLogger.java`

**Why we need it:** Redis is in-memory. When the server restarts, all data is gone — unless we saved it somewhere.

**AOF (Append-Only File)** is the simplest possible persistence: every write command is appended to a file on disk in RESP format.

### Write path

```java
// AofLogger.java
public synchronized void append(byte[] data) throws IOException {
    output.write(data);
    output.flush();      // ← flush to OS immediately
}
```

`synchronized` means only one thread can write to the file at a time — no corrupt interleaving.

The `RespCommand` carries its original raw bytes (`command.getRawBytes()`), so we write exactly what the client sent — no re-serialization needed.

### Replay on startup

```java
// AofLogger.java
public void replay(CommandHandler commandHandler) throws IOException {
    RespParser parser = new RespParser(new FileInputStream(filePath));
    while (true) {
        RespCommand command = parser.readCommand();
        if (command == null) break;         // end of file
        commandHandler.execute(command);    // re-execute every stored command
    }
}
```

**Before replay:** `database.setRecovering(true)` — this stops the replay from writing back to the AOF file (otherwise the file doubles on every restart).

**After replay:** `database.setRecovering(false)` — normal operation resumes.

---

## Step 6 — The TCP Server (Entry Point)

**File:** `src/com/redis/server/RedisServer.java`

This is the `main()` method — where everything starts.

```java
// RedisServer.java — simplified
ServerSocket serverSocket = new ServerSocket(6379);

while (!serverSocket.isClosed()) {
    Socket clientSocket = serverSocket.accept();  // blocks until client connects
    
    connectionPool.submit(
        new ConnectionTask(clientSocket),
        100, TimeUnit.MILLISECONDS
    );
}
```

`serverSocket.accept()` **blocks** the main thread until a client connects. When one does, it wraps the socket in a `ConnectionTask` and hands it to the `ConnectionPool`.

**Why not create a new Thread per connection?**
> If 1000 clients connect simultaneously, 1000 threads is too expensive. A thread pool with 4 workers handles all of them from a queue — this is the classic **thread pool pattern**.

### Shutdown Hook

```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    serverSocket.close();      // stop accepting new connections
    router.shutdown();         // drain all partition queues
    connectionPool.shutdown(); // drain in-flight handlers
    expirationScheduler.shutdown();
}));
```

When you press Ctrl+C, the JVM calls this hook. It drains all in-flight work before the process exits — a **graceful shutdown**.

---

## Step 7 — The Connection Pool

**File:** `src/com/redis/pool/ConnectionPool.java`

**OS Concept:** Thread pool — fixed set of threads consuming work from a shared queue.

### Why not `Executors.newFixedThreadPool()`?

We built our own pool because:
1. We need **backpressure** — if the queue is full, reject with an error (not silently block forever)
2. We need **metrics** — how many workers are active, how long tasks waited
3. We need **graceful shutdown** — drain the queue before exiting (poison pill pattern)
4. It demonstrates OS knowledge better

### How it works

```java
// ConnectionPool.java — constructor
for (int i = 0; i < workerCount; i++) {
    Thread thread = new Thread(() -> workerLoop(info), "connection-worker-" + i);
    thread.setDaemon(false);  // ← must NOT die before shutdown drains
    thread.start();
}
```

**workerLoop** (the core pattern):

```java
private void workerLoop(WorkerInfo info) {
    while (true) {
        ConnectionTask task = queue.take();    // BLOCKS here when queue is empty
        
        if (task == POISON_PILL) break;        // time to exit
        
        // Run the full client session
        new RedisClientHandler(task.getSocket(), ...).run();
        // ^ This blocks until the client disconnects
    }
}
```

Each worker handles **one client at a time** — it stays inside `run()` for the entire client session (reading commands, routing, writing responses). When the client disconnects, `run()` returns, the worker loops back to `queue.take()` and waits for the next client.

---

## Step 8 — BoundedTaskQueue (The Queue Itself)

**File:** `src/com/redis/pool/BoundedTaskQueue.java`

**OS Concept:** Bounded buffer with producer-consumer synchronization using a monitor (lock + condition variables).

This is a **hand-written** bounded blocking queue — the same data structure as `java.util.concurrent.ArrayBlockingQueue` but written from scratch to demonstrate OS knowledge.

### Internal structure

```java
private final Deque<T>       queue    = new ArrayDeque<>();
private final int            capacity;
private final ReentrantLock  lock     = new ReentrantLock();
private final Condition      notEmpty = lock.newCondition(); // signal when item added
private final Condition      notFull  = lock.newCondition(); // signal when item removed
```

**`ReentrantLock` + `Condition`** is the Java equivalent of a **monitor** from OS theory.

### Producer side — `offer()` with timeout

```java
public boolean offer(T task, long timeout, TimeUnit unit) throws InterruptedException {
    long nanos = unit.toNanos(timeout);
    lock.lockInterruptibly();      // acquire the monitor
    try {
        while (queue.size() == capacity) {   // queue is FULL
            if (nanos <= 0) return false;    // timeout expired — reject
            nanos = notFull.awaitNanos(nanos); // wait for space
        }
        queue.addLast(task);
        notEmpty.signal();         // wake up a waiting consumer
        return true;
    } finally {
        lock.unlock();
    }
}
```

If the queue is full, the producer **waits** (releases the lock, sleeps) for up to the timeout. If space appears, `notFull.signal()` wakes it up. If timeout expires first, it returns `false` → `TaskRejectedException`.

### Consumer side — `take()`

```java
public T take() throws InterruptedException {
    lock.lockInterruptibly();
    try {
        while (queue.isEmpty()) {
            notEmpty.await();      // wait until something is added
        }
        T task = queue.removeFirst();
        notFull.signal();          // wake up a waiting producer
        return task;
    } finally {
        lock.unlock();
    }
}
```

Workers block here when the queue is empty. When a producer adds an item and calls `notEmpty.signal()`, one sleeping worker wakes up and processes it.

### The Poison Pill pattern

Shutdown cannot just interrupt worker threads — an interrupted thread might drop work in progress. Instead, we enqueue a **sentinel value** (poison pill):

```java
private static final Runnable POISON_PILL = () -> {};  // unique identity object

// Shutdown: add one pill per worker
for (int i = 0; i < workerCount; i++) {
    queue.put(POISON_PILL);
}
```

Each worker, when it dequeues the pill, recognizes it (`task == POISON_PILL` — identity comparison, not equals) and exits cleanly. Any real tasks queued before the pill are processed first.

---

## Step 9 — Command Router

**File:** `src/com/redis/partition/CommandRouter.java`

**Why we need it:** one global lock on the database would be a bottleneck. If a LPUSH and a HSET are happening simultaneously, they don't touch the same data — why should they wait for each other?

**Solution:** route by data type. STRING commands go to the STRING partition, LIST commands go to the LIST partition, etc.

```java
// CommandRouter.java
private static DataType classify(String verb) {
    return switch (verb.toUpperCase()) {
        case "GET","SET","DEL","INCR","DECR",... -> DataType.STRING;
        case "LPUSH","RPUSH","LPOP","RPOP",...   -> DataType.LIST;
        case "HSET","HGET","HDEL",...             -> DataType.HASH;
        case "SADD","SREM","SMEMBERS",...         -> DataType.SET;
        case "ZADD","ZREM","ZRANGE",...            -> DataType.ZSET;
        default -> DataType.STRING;
    };
}
```

### The Shared Semaphore

```java
// CommandRouter.java constructor
private final Semaphore dbPermits = new Semaphore(8, true); // fair=true
```

**What this does:** limits the total number of commands running against the database simultaneously to 8, across all 5 partitions combined.

- 12 partition workers exist, but only 8 can be inside the database at the same time
- `fair=true` means threads acquire permits in arrival order (FIFO) — no starvation
- Without this, all 12 workers could hit the database at once under high load

**Why fair semaphore?** Without fairness, a lucky thread could keep re-acquiring the permit repeatedly, starving others. Fair = each thread waits its turn.

---

## Step 10 — DataTypePartition

**File:** `src/com/redis/partition/DataTypePartition.java`

**OS Concept:** Worker thread pool with per-key ordering and a shared semaphore.

Each partition (STRING, LIST, HASH, SET, ZSET) is a `DataTypePartition`. It owns:
- A `PartitionedQueue` (N sub-queues, one per worker)
- N worker threads (N = 4 for STRING, 2 for others)
- Access to the shared `dbPermits` semaphore

### Key hashing for ordering

```java
// PartitionedQueue.java
public int partitionIndex(String key) {
    return Math.abs(key.hashCode()) % partitionCount;
}
```

`SET name Mohan` and `GET name` will always hash to the **same partition worker**. This guarantees that if you SET a key and immediately GET it, the GET executes after the SET — no race condition.

Two different keys (e.g. `name` and `age`) might hash to different workers and run in parallel — that's the concurrency benefit.

### Worker loop in DataTypePartition

```java
private void workerLoop(WorkerInfo info, int partitionIndex) {
    while (true) {
        Runnable task = queue.take(partitionIndex); // blocks on THIS worker's queue
        
        if (task == POISON_PILL) break;
        
        dbPermits.acquire();          // OS CONCEPT: semaphore acquire
        try {
            task.run();               // executes CommandHandler → RedisDatabase
        } finally {
            dbPermits.release();      // OS CONCEPT: semaphore release
        }
    }
}
```

`task.run()` calls `CommandHandler.execute()` which calls the right store method and returns a `CommandResult`. The task is a lambda (closure) that also holds a `CompletableFuture` reference — it calls `future.complete(result)` when done.

---

## Step 11 — RedisClientHandler — Connecting Everything

**File:** `src/com/redis/server/RedisClientHandler.java`

This is the most important file to understand because it ties everything together.

### The normal flow

```java
// RedisClientHandler.java — run() simplified
RespParser parser = new RespParser(socket.getInputStream());
RespWriter  writer = new RespWriter(socket.getOutputStream());

while (true) {
    RespCommand command = parser.readCommand();   // 1. decode from socket
    if (command == null) break;                   //    null = client disconnected

    CommandResult result = dispatchAndWait(command, verb, key); // 2. execute
    writer.write(result);                         // 3. encode and send back
}
```

### dispatchAndWait — The CompletableFuture Bridge

This is the key design. The connection worker thread submits work to a partition worker thread, then **waits** for the result:

```java
private CommandResult dispatchAndWait(RespCommand command, String verb, String key) {
    
    // 1. Create a "promise" for the result
    CompletableFuture<CommandResult> future = new CompletableFuture<>();
    
    // 2. Submit a lambda to the router
    //    This lambda runs on a PARTITION WORKER thread (not this thread)
    router.route(verb, key, () -> {
        
        CommandResult result = commandHandler.execute(command);  // executes the command
        
        if (shouldPersist(verb, result)) {
            database.persist(command);  // append to AOF file
        }
        
        future.complete(result);        // signal the waiting connection worker
        
    }, 100, TimeUnit.MILLISECONDS);
    
    // 3. THIS THREAD blocks here until future.complete() is called above
    return future.join();
}
```

**Why CompletableFuture?**
The connection worker (Thread A) needs the result of work done by a partition worker (Thread B). `CompletableFuture` is the bridge:
- Thread A calls `future.join()` → parks itself (sleeps, releases CPU)
- Thread B calls `future.complete(result)` → wakes Thread A up
- Thread A receives the result and returns it

This is **inter-thread communication** — an OS concept.

### BLPOP — Blocking Pop

BLPOP is special: if the list is empty, the client should **wait** until another client pushes something.

```java
// RedisClientHandler.java
private CommandResult executeBlockingListPop(...) {
    
    // Try immediately first
    CommandResult initial = dispatchAndWait(command, verb, key);
    if (initial.getType() != RespType.NULL) return initial;  // got something
    
    // List was empty — register as a waiter
    CompletableFuture<Void> signal = new CompletableFuture<>();
    database.registerListWaiter(key, signal);
    
    // Wait for a signal (another client does LPUSH)
    signal.get(waitMs, TimeUnit.MILLISECONDS);
    
    // Try again after being woken up
    return dispatchAndWait(command, verb, key);
}
```

When another client does `LPUSH mylist value`:
```java
// ListStore.java — after pushing
waiters.forEach(f -> f.complete(null));  // wake up all BLPOP waiters
```

This is the **condition variable** pattern from OS: wait on a condition, signal when the condition changes.

---

## Step 12 — Monitoring

**File:** `src/com/redis/pool/RedisPoolMonitor.java`, `PoolMetrics.java`, `PoolHealth.java`, `WorkerInfo.java`

Every 5 seconds, `RedisPoolMonitor` prints a dashboard:

```
╔══════════════════════════════════╗
║  Pool: connection-pool           ║
║  Workers: 4  Active: 2  Queue: 3/8║
║  Submitted: 142  Completed: 139  ║
║  Health: HEALTHY                 ║
╚══════════════════════════════════╝
```

`PoolMetrics` is an **immutable snapshot** — it captures the pool state at one point in time. It uses `AtomicLong` counters (lock-free, thread-safe) for the numbers.

`PoolHealth` can be: `HEALTHY`, `DEGRADED` (queue >80% full), `OVERLOADED` (queue full + rejections), or `SHUTTING_DOWN`.

`WorkerInfo` tracks each individual thread: its name, current state (WAITING / RUNNING / FAILED / STOPPED), and when it last started/finished a task.

---

## The Complete Thread Map

When the server is running, these threads exist:

| Thread Name | Count | What it does |
|-------------|-------|-------------|
| `main` | 1 | `accept()` loop — waits for new TCP connections |
| `connection-worker-0..3` | 4 | Runs `RedisClientHandler` for active clients |
| `string-worker-0..3` | 4 | Executes STRING commands against StringStore |
| `list-worker-0..1` | 2 | Executes LIST commands against ListStore |
| `hash-worker-0..1` | 2 | Executes HASH commands against HashStore |
| `set-worker-0..1` | 2 | Executes SET commands against SetStore |
| `zset-worker-0..1` | 2 | Executes ZSET commands against SortedSetStore |
| `expiration-sweeper` | 1 | Removes expired keys every second |
| `pool-monitor` | 1 | Prints pool metrics every 5 seconds |
| `shutdown-hook` | 1 | Runs on Ctrl+C to drain queues gracefully |

**Total: 20 threads** running simultaneously.

---

## OS Concepts Used — Exam Cheat Sheet

| Concept | Where Used | Java Class |
|---------|-----------|------------|
| **Thread** | Every pool worker, sweeper, monitor | `Thread` |
| **Thread Pool** | ConnectionPool, DataTypePartition | Custom built from scratch |
| **Semaphore** | `dbPermits` — cap concurrent DB ops | `Semaphore(8, true)` |
| **Mutex / Monitor** | Queue access — only one thread at a time | `ReentrantLock` |
| **Condition Variable** | Producer/consumer wait/signal in queue | `Condition` (notFull, notEmpty) |
| **Blocking Queue** | BoundedTaskQueue — bounded buffer | Custom, uses lock + condition |
| **Bounded Buffer** | BoundedTaskQueue with capacity limit | Same as above |
| **Poison Pill** | Graceful shutdown of worker threads | Sentinel `Runnable POISON_PILL` |
| **Atomic Counter** | Lock-free metrics counting | `AtomicLong`, `AtomicInteger` |
| **CompletableFuture** | Bridge between connection and partition worker | `CompletableFuture<CommandResult>` |
| **Inter-thread Signaling** | BLPOP waiter woken by LPUSH | `CompletableFuture.complete()` |
| **Daemon thread** | Sweeper/monitor die with JVM | `thread.setDaemon(true)` |
| **Non-daemon thread** | Workers must drain before JVM exits | `thread.setDaemon(false)` |
| **Shutdown Hook** | Graceful cleanup on Ctrl+C | `Runtime.addShutdownHook()` |
| **File I/O / fsync** | AOF persistence | `BufferedOutputStream + flush()` |
| **Active Expiry** | Background sweep of expired keys | `ScheduledExecutorService` |
| **Lazy Expiry** | Check TTL on each access | Checked in every store `get()` |

---

## How to Explain Each File in 1 Sentence

| File | One-line explanation |
|------|----------------------|
| `RedisServer.java` | Opens port 6379, accepts TCP connections, hands each one to the ConnectionPool |
| `RedisClientHandler.java` | Reads RESP commands from one client, routes them to partition workers, writes responses back |
| `RespParser.java` | Decodes raw bytes from the client into a `String[]` command array using the RESP protocol |
| `RespWriter.java` | Encodes a `CommandResult` back into RESP bytes and writes them to the socket |
| `CommandRouter.java` | Classifies every command verb into a data type and routes it to the right partition, enforcing an 8-permit semaphore |
| `DataTypePartition.java` | Owns N worker threads and N queues for one data type; workers acquire the shared semaphore before touching the database |
| `ConnectionPool.java` | Manages 4 worker threads that handle client sessions; built with BoundedTaskQueue and poison-pill shutdown |
| `BoundedTaskQueue.java` | A hand-written bounded blocking queue using ReentrantLock + Condition (producer/consumer monitor pattern) |
| `RedisWorkerPool.java` | Earlier version of the worker pool; still used as a reference implementation with its own internal semaphore |
| `CommandHandler.java` | Dispatches parsed commands to the right typed handler (StringCommandHandler, ListCommandHandler, etc.) |
| `RedisDatabase.java` | Facade over all five data-type stores; also manages cross-type expiry and routes AOF logging |
| `AofLogger.java` | Appends every write command to `redis.aof` on disk; replays the file on startup to restore state |
| `StringStore.java` | Stores string values in a ConcurrentHashMap with TTL/expiry support |
| `ListStore.java` | Stores per-key LinkedLists; signals BLPOP waiters when a push happens |
| `HashStore.java` | Stores per-key HashMaps for HSET/HGET/HGETALL operations |
| `SetStore.java` | Stores per-key HashSets; supports SUNION/SINTER/SDIFF set operations |
| `SortedSetStore.java` | Stores per-key sorted sets with dual-index for score and lexicographic range queries |
| `ExpirationManager.java` | Tracks expiry timestamps for non-string types; shared across List/Hash/Set/ZSet stores |
| `RedisPoolMonitor.java` | Background thread that prints pool health, queue depths, and worker states every 5 seconds |
| `PoolMetrics.java` | Immutable snapshot of pool statistics at a point in time |
| `PoolHealth.java` | Enum: HEALTHY / DEGRADED / OVERLOADED / SHUTTING_DOWN, computed from queue fill and rejection rate |
| `WorkerInfo.java` | Tracks one worker thread's name, current state, and last task timestamps |
| `WorkerState.java` | Enum: WAITING / RUNNING / FAILED / STOPPING / STOPPED |
| `InteractiveClient.java` | REPL client — reads a command from stdin, builds RESP bytes, sends to server, prints response |
| `StressTest.java` | Multi-threaded load test that fires N commands per thread and measures throughput |
