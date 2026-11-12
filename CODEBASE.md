# Codebase Map — What Every File Does

This document explains every file in the project, grouped by layer.

---

## Project Layout

```
src/com/redis/
├── db/
│   ├── handler/          ← One handler class per data type
│   └── store/            ← One store class per data type (actual data structures)
├── partition/            ← Routes commands to the right partition/worker
├── pool/                 ← Thread pool, queues, monitoring
├── protocol/             ← RESP protocol parsing and writing
├── server/               ← TCP server, per-connection handler, AOF logger
└── stress/               ← Load testing and interactive client
```

---

## `server/` — The TCP Server

| File | What it does |
|------|-------------|
| `RedisServer.java` | Entry point. Opens a `ServerSocket` on port 6379, accepts incoming TCP connections in a loop, and hands each connection off to a new `RedisClientHandler` thread. |
| `RedisClientHandler.java` | Runs per-connection in its own thread. Reads bytes from the socket, feeds them to `RespParser` to decode commands, routes each command through `CommandRouter`, gets back a `CommandResult`, and writes the response via `RespWriter`. Closes the socket when the client disconnects. |
| `AofLogger.java` | Append-Only File persistence. Every write command that succeeds gets appended to `redis.aof` in RESP format. On server startup, it replays the AOF file by re-executing every stored command so the in-memory state is restored. |

---

## `protocol/` — The RESP Wire Protocol

| File | What it does |
|------|-------------|
| `RespParser.java` | Parses raw bytes from the client into a `RespCommand`. Handles all RESP types: `+` simple string, `-` error, `:` integer, `$` bulk string, `*` array. Used by `RedisClientHandler` to decode incoming commands. |
| `RespWriter.java` | Serializes a `CommandResult` back into RESP bytes and writes them to the socket output stream. Called after every command execution. |
| `RespCommand.java` | A plain data class holding a parsed command as a `String[]` array (e.g. `["SET", "name", "Mohan"]`). |
| `RespType.java` | Enum of the 5 RESP wire types: `SIMPLE_STRING`, `ERROR`, `INTEGER`, `BULK_STRING`, `ARRAY`. Used during parsing and writing. |

---

## `partition/` — Command Routing & Concurrency

| File | What it does |
|------|-------------|
| `CommandRouter.java` | The traffic cop. Maps every command verb to a `DataType`, then submits the work to the right `DataTypePartition`. Holds a shared `Semaphore` (8 permits) that caps total concurrent database operations across the whole server. Also provides `shutdown()`, `printMetrics()`, and stats accessors. |
| `DataTypePartition.java` | Manages a set of worker threads (e.g. 4 for STRING, 2 for LIST) and a `PartitionedQueue`. Accepts tasks via `submit(key, task, timeout, unit)`, hashes the key to pick a worker, and enqueues the work. Tracks submitted/completed/rejected counters and wait-time metrics. |
| `DataType.java` | Simple enum: `STRING`, `LIST`, `HASH`, `SET`, `ZSET`. |

---

## `pool/` — Thread Pool & Monitoring

| File | What it does |
|------|-------------|
| `RedisWorkerPool.java` | A fixed thread pool where each worker pulls tasks from a `BoundedTaskQueue` and executes them. This is what `DataTypePartition` uses internally for each of its worker threads. |
| `BoundedTaskQueue.java` | A blocking queue with a maximum capacity. Producers block (up to a timeout) if the queue is full; consumers block when it's empty. Used per-worker inside a partition. |
| `PartitionedQueue.java` | Holds multiple `BoundedTaskQueue` instances (one per worker slot) and routes incoming tasks to them by key hash, keeping per-key ordering. |
| `ConnectionPool.java` | Manages a pool of reusable client connections. Used by the stress test clients to avoid creating a new socket per request. |
| `ConnectionTask.java` | Wraps a single unit of work to be executed over a pooled connection. |
| `MonitorablePool.java` | Interface that any pool must implement to expose health and metrics. Defines `getMetrics()` and `getHealth()`. |
| `PoolMetrics.java` | Data class holding pool statistics: active workers, queued tasks, completed tasks, rejected tasks, average wait times. |
| `PoolHealth.java` | Data class describing pool health state: `HEALTHY`, `DEGRADED`, or `OVERLOADED`, plus a reason string. |
| `WorkerInfo.java` | Snapshot of a single worker thread's current state: thread name, state, tasks completed, current task. |
| `WorkerState.java` | Enum for a worker's lifecycle: `IDLE`, `BUSY`, `SHUTDOWN`. |
| `RedisPoolMonitor.java` | Periodically samples `PoolMetrics` and `PoolHealth` from all pools and prints a dashboard to stdout. Runs in its own background thread. |
| `RedisPoolStressTest.java` | A standalone stress test that hammers the pool directly (bypassing the TCP layer) to validate queue behavior, rejection handling, and backpressure under load. |
| `TaskRejectedException.java` | Thrown when a partition's queue is full and a task cannot be accepted within the timeout. `RedisClientHandler` catches this and returns an error to the client. |

---

## `db/` — The Database

| File | What it does |
|------|-------------|
| `RedisDatabase.java` | The facade for all data operations. All five `CommandHandler` classes call methods on this class. It delegates to the appropriate store (`StringStore`, `ListStore`, etc.) and the `ExpirationManager`. |
| `CommandHandler.java` | Top-level command dispatcher. Receives a parsed `String[]` command and routes it to the right typed handler (`StringCommandHandler`, `ListCommandHandler`, etc.) using a `switch` on the verb. |
| `CommandResult.java` | Immutable result value returned by every command. Factory methods: `simpleString()`, `bulkString()`, `integer()`, `array()`, `nil()`, `error()`. `RespWriter` reads this to build the wire response. |
| `RedisEntry.java` | Wraps a stored value with its type tag and optional expiry timestamp (milliseconds). Used by `StringStore`. |
| `SetOptions.java` | Plain data class parsed from a `SET` command's optional flags: `nx`, `xx`, `expirationTimeMillis`. |

### `db/handler/` — Per-Type Command Handlers

| File | What it does |
|------|-------------|
| `StringCommandHandler.java` | Handles: `SET`, `GET`, `DEL`, `INCR`, `DECR`, `MGET`, `MSET`, `SETNX`, `GETSET`, `GETDEL`, `GETEX`, `APPEND`, `STRLEN`, `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `PERSIST`. Parses arguments, validates arity, calls `RedisDatabase`, returns `CommandResult`. |
| `ListCommandHandler.java` | Handles: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LREM`, `LINSERT`, `LTRIM`, `BLPOP`, `BRPOP`. |
| `HashCommandHandler.java` | Handles: `HSET`, `HMSET`, `HGET`, `HMGET`, `HDEL`, `HGETALL`, `HKEYS`, `HVALS`, `HEXISTS`, `HLEN`, `HINCRBY`, `HINCRBYFLOAT`. |
| `SetCommandHandler.java` | Handles: `SADD`, `SREM`, `SISMEMBER`, `SCARD`, `SMEMBERS`, `SUNION`, `SINTER`, `SDIFF`, `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE`, `SMOVE`, `SRANDMEMBER`, `SPOP`. |
| `SortedSetCommandHandler.java` | Handles: `ZADD`, `ZREM`, `ZRANGE`, `ZREVRANGE`, `ZSCORE`, `ZCARD`, `ZRANK`, `ZREVRANK`, `ZINCRBY`, `ZRANGEBYSCORE`, `ZCOUNT`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZRANGEBYLEX`, `ZLEXCOUNT`. |

### `db/store/` — Actual Data Structures

| File | What it does |
|------|-------------|
| `StringStore.java` | Stores string values in a `ConcurrentHashMap<String, RedisEntry>`. Implements get/set/del/incr/decr/append/strlen/expiry. |
| `ListStore.java` | Stores lists as `LinkedList<String>` per key. Implements lpush/rpush/lpop/rpop/lrange/lindex/lset/lrem/linsert/ltrim plus blocking pop with a `Condition`. |
| `HashStore.java` | Stores hashes as `HashMap<String, String>` per key. Implements all HSET/HGET/HDEL/HGETALL/HKEYS/HVALS/HINCRBY variants. |
| `SetStore.java` | Stores sets as `HashSet<String>` per key. Implements sadd/srem/smembers/sismember/scard/sunion/sinter/sdiff and their store variants. |
| `SortedSetStore.java` | Stores sorted sets. Implements all ZADD/ZREM/ZRANGE/ZSCORE/ZRANK/ZINCRBY/ZRANGEBYSCORE variants. |
| `SortedSet.java` | The sorted set data structure itself — dual-indexed with a `HashMap` for O(1) score lookup and a sorted structure for range queries. |
| `ExpirationManager.java` | Tracks TTL expiry timestamps. Called by stores to check whether a key has expired (lazy expiry — checked on access). Also provides `persist()`, `expire()`, `ttl()`, `pttl()`. |

---

## `stress/` — Clients & Load Testing

| File | What it does |
|------|-------------|
| `RedisClient.java` | Simple hardcoded client. Opens a socket, sends `SET name Mohan` → `GET name` → `DEL name` → `GET name`, prints responses, closes. Good for a quick smoke test. |
| `StressTest.java` | Orchestrates a multi-threaded load test: spins up N threads each firing M commands (mix of SET/GET/INCR) as fast as possible, then reports throughput and latency. |
| `StressTestClient.java` | A single stress-test worker. Owns one TCP connection, fires commands in a loop, tracks per-command latency. |
| `InteractiveClient.java` | A REPL client you can type commands into. Reads a line, splits on spaces (quote-aware), builds a RESP array, sends it, pretty-prints the response. Type `EXIT` to quit. |

---

## Root Files

| File | What it does |
|------|-------------|
| `sources.txt` | List of all `.java` source files, used with `javac -d out @sources.txt` to compile the project without a build tool. |
| `redis.aof` | The Append-Only File written by `AofLogger`. Contains every write command the server has ever executed, in RESP format. Replayed on startup to restore state. |
| `.gitignore` | Excludes `out/` (compiled classes) and IDE config from version control. |
