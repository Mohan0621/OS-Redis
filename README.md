# Redis Clone

A multi-threaded, Redis-compatible in-memory database server built from scratch in Java. Implements the RESP wire protocol, five Redis data types, AOF persistence, and a partitioned concurrency model — designed as a practical demonstration of operating-system concepts (thread pools, semaphores, monitors, bounded buffers, and graceful shutdown).

---

## Features

- **Full RESP protocol** — compatible with any standard Redis client over TCP (port 6379)
- **5 data types** — String, List, Hash, Set, Sorted Set
- **51 commands** — including `SET` with NX/XX/EX/PX options, `BLPOP`/`BRPOP` blocking pops, `ZRANGEBYLEX`, `HINCRBYFLOAT`, and more
- **AOF persistence** — every write command is appended to `redis.aof` and replayed on startup for crash recovery
- **TTL & expiry** — `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, plus a background expiration sweeper
- **Partitioned concurrency** — each data type has its own worker pool, so cross-type operations never contend
- **Backpressure** — bounded queues reject work with `TaskRejectedException` instead of growing unbounded
- **Graceful shutdown** — poison-pill pattern drains in-flight work on Ctrl+C before the JVM exits
- **Live monitoring** — background thread prints pool health, queue depths, and worker state every 5 seconds
- **Interactive REPL client** — `InteractiveClient` for manual testing from the command line

---

## Architecture

```
TCP Client  →  RedisServer (accept loop)
                ↓
             ConnectionPool  (4 workers, queue=8)
                ↓
             RedisClientHandler  (per-connection, RESP parse → route → write)
                ↓
             CommandRouter  (classify verb → DataType, shared Semaphore(8))
                ↓
             DataTypePartition  (STRING:4, LIST:2, HASH:2, SET:2, ZSET:2)
                ↓
             CommandHandler → RedisDatabase → typed store
                ↓
             AofLogger → redis.aof (disk)
```

The server runs **20 threads** simultaneously: 1 accept loop, 4 connection workers, 12 partition workers, 1 expiration sweeper, 1 pool monitor, and 1 shutdown hook.

A shared `Semaphore(8, fair)` caps total concurrent database operations across all partitions, preventing thundering-herd behavior under spike load. Within each partition, keys are hashed to a specific worker, preserving per-key ordering without a global lock.

For a detailed walkthrough of every component and design decision, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Quick Start

### Prerequisites

- **Java 17** or later
- **Maven 3.6+** (or use the included `sources.txt` with `javac`)

### Build

```bash
mvn clean package
```

Or without Maven:

```bash
javac -d out @sources.txt
```

### Run the Server

```bash
mvn exec:java
```

Or without Maven:

```bash
java -cp out com.redis.server.RedisServer
```

The server starts on **port 6379** and replays `redis.aof` (if present) before accepting connections.

### Connect a Client

**Interactive REPL client (built-in):**

```bash
java -cp out com.redis.stress.InteractiveClient
```

```
> SET name Mohan EX 60
OK
> GET name
"Mohan"
> HSET user:1 name Mohan age 21 city Hyderabad
(integer) 3
> HGETALL user:1
1) "name"
2) "Mohan"
3) "age"
4) "21"
5) "city"
6) "Hyderabad"
> ZADD scores 300 alice 150 bob 200 charlie
(integer) 3
> ZREVRANGE scores 0 -1 WITHSCORES
1) "alice"
2) "300.0"
3) "charlie"
4) "200.0"
5) "bob"
6) "150.0"
```

**Or use the standard `redis-cli`** (if Redis is installed locally):

```bash
redis-cli -p 6379
```

---

## Supported Commands

51 commands across 6 categories:

| Category | Count | Commands |
|----------|-------|----------|
| String + TTL | 19 | `SET` (NX/XX/EX/PX), `GET`, `DEL`, `INCR`, `DECR`, `MSET`, `MGET`, `SETNX`, `GETSET`, `GETDEL`, `GETEX`, `APPEND`, `STRLEN`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `PING` |
| List | 13 | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LREM`, `LINSERT`, `LTRIM`, `BLPOP`, `BRPOP` |
| Hash | 12 | `HSET`, `HMSET`, `HGET`, `HMGET`, `HDEL`, `HGETALL`, `HKEYS`, `HVALS`, `HEXISTS`, `HLEN`, `HINCRBY`, `HINCRBYFLOAT` |
| Set | 14 | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SUNION`, `SINTER`, `SDIFF`, `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE`, `SMOVE`, `SRANDMEMBER`, `SPOP` |
| Sorted Set | 15 | `ZADD`, `ZREM`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREVRANGE`, `ZINCRBY`, `ZRANGEBYSCORE`, `ZCOUNT`, `ZRANGEBYLEX`, `ZLEXCOUNT`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZCARD` |

Full syntax and examples for every command: [COMMANDS.md](COMMANDS.md).

---

## Project Structure

```
src/com/redis/
├── server/          TCP server, per-connection handler, AOF logger
├── protocol/        RESP parser, writer, command, type enum
├── partition/        Command router, DataType partition, DataType enum
├── pool/             Worker pool, bounded queue, connection pool, monitoring
├── db/
│   ├── handler/      One handler per data type
│   └── store/        One store per data type + expiration manager
└── stress/           Interactive client + stress test harness
```

For a file-by-file breakdown, see [CODEBASE.md](CODEBASE.md).

---

## OS Concepts Demonstrated

This project applies operating-system primitives to a real, working server instead of toy examples:

| OS Concept | Where Used | Java Primitive |
|-----------|-----------|----------------|
| Thread pool | ConnectionPool, DataTypePartition | Custom (built from `Thread`) |
| Semaphore (fair) | Cross-partition DB concurrency cap | `Semaphore(8, true)` |
| Monitor / mutex | BoundedTaskQueue access | `ReentrantLock` |
| Condition variable | Producer/consumer wait-signal | `Condition` (`notEmpty`, `notFull`) |
| Bounded buffer | Task queues with capacity limit | `BoundedTaskQueue` |
| Poison-pill shutdown | Graceful worker termination | Sentinel identity check |
| Atomic counters | Lock-free metrics | `AtomicLong`, `AtomicInteger` |
| Inter-thread signaling | BLPOP waiter wakeup | `CompletableFuture` |
| Background scheduled task | Expired-key sweeper | `ScheduledExecutorService` |
| Graceful shutdown hook | Ctrl+C cleanup | `Runtime.addShutdownHook()` |
| File I/O & durability | AOF persistence | `BufferedOutputStream` + `flush()` |

---

## Configuration

Defaults are defined as constants in `RedisServer.java`:

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `PORT` | 6379 | TCP listen port |
| `CONN_WORKERS` | 4 | Connection-handler threads |
| `CONN_QUEUE_CAP` | 8 | Max pending connections |
| `DB_PERMITS` | 8 | Concurrent database operations |
| `MONITOR_INTERVAL` | 5000 ms | Pool metrics print interval |
| `OFFER_TIMEOUT_MS` | 100 ms | Max wait for queue space |

Partition worker counts are set in `CommandRouter.java`: STRING=4, LIST=2, HASH=2, SET=2, ZSET=2 (12 total).

---

## Limitations & Known Gaps

This is an educational implementation. The following Redis features are **not** implemented:

- `PUBLISH` / `SUBSCRIBE` (pub-sub)
- `MULTI` / `EXEC` (transactions)
- `AUTH` (password authentication)
- `KEYS` / `SCAN` (admin commands)
- `CONFIG GET/SET`, `INFO`
- Replication and clustering

Known issues:

- `RespWriter` flushes per token rather than per full response (minor perf hit on array replies)
- AOF crash-truncation is not handled on replay (a partial last line will throw)
- Sorted set backing structure has a duplicate-score edge case

---

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — deep-dive walkthrough of every component, design decision, and OS concept
- [COMMANDS.md](COMMANDS.md) — full syntax and examples for all 51 commands
- [CODEBASE.md](CODEBASE.md) — what every `.java` file does, organized by layer

---

## Tech Stack

- **Java 17** (switch expressions, text blocks, `var`)
- **Maven** build with JUnit 5
- **Zero external runtime dependencies** — standard library only
- **No `java.util.concurrent` executors** for the core pools — built from raw `Thread`, `ReentrantLock`, `Condition`, and `Semaphore` to make the OS primitives explicit

---

## Contributors

- **Chaitanya MVR**
- **Mohan**

Built as an Operating Systems course project.

---

## License

This project is for educational purposes. No warranty — see the known limitations above before relying on it for anything real.
