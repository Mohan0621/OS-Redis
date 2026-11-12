package com.redis.partition;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import com.redis.pool.TaskRejectedException;

/**
 * Routes incoming Redis commands to the correct DataTypePartition.
 *
 * Role in the architecture
 * ------------------------
 *
 *   TCP Client
 *       │
 *       ▼
 *   RespParser  (in RedisClientHandler)
 *       │
 *       ▼
 *   CommandRouter.route(key, task, timeout, unit)
 *       │
 *       ├─ STRING commands ──▶ stringPartition.submit(key, task)
 *       ├─ LIST   commands ──▶ listPartition.submit(key, task)
 *       ├─ HASH   commands ──▶ hashPartition.submit(key, task)
 *       ├─ SET    commands ──▶ setPartition.submit(key, task)
 *       └─ ZSET   commands ──▶ zsetPartition.submit(key, task)
 *
 * Shared semaphore
 * ----------------
 * All five partitions share one Semaphore (dbPermits).  This caps the
 * total number of concurrent data-store operations across the entire
 * server regardless of how many partitions or workers exist.
 *
 *   partitions × workers  =  many possible concurrent ops
 *   dbPermits             =  actual concurrent ops allowed
 *
 * This is where the semaphore finally earns its place: it's a real
 * resource gate, not just a teaching example.
 *
 * Command → DataType mapping
 * --------------------------
 * STRING : GET, SET, DEL, INCR, DECR, MGET, MSET, SETNX, GETSET,
 *          APPEND, STRLEN, TTL, PERSIST, EXPIRE, PEXPIRE
 * LIST   : LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LREM
 * HASH   : HSET, HGET, HDEL, HGETALL, HMSET, HMGET, HKEYS, HVALS, HLEN
 * SET    : SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SUNION, SINTER, SDIFF
 * ZSET   : ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZRANGEBYSCORE, ZCARD,
 *          ZINCRBY, ZREVRANK, ZREVRANGE
 *
 * Unknown commands fall through to the STRING partition (safest default
 * because that's where GET/SET live and it avoids a hard NPE).
 *
 * Partition configuration (production-ish defaults)
 * --------------------------------------------------
 * STRING: 4 partitions — highest traffic type
 * LIST:   2 partitions — moderate traffic
 * HASH:   2 partitions
 * SET:    2 partitions
 * ZSET:   2 partitions
 * Total:  12 workers → semaphore permits = 8 (two-thirds utilisation cap)
 */
public class CommandRouter {

    // ----------------------------------------------------------------
    // Shared database concurrency gate
    // ----------------------------------------------------------------

    /**
     * Fair semaphore shared by all DataTypePartitions.
     * Limits total simultaneous data-store accesses across the server.
     *
     * Sizing heuristic: (total workers) × 0.67
     * Here: 12 workers → 8 permits.
     */
    private final Semaphore dbPermits;

    // ----------------------------------------------------------------
    // One partition per data type
    // ----------------------------------------------------------------

    private final EnumMap<DataType, DataTypePartition> partitions =
            new EnumMap<>(DataType.class);

    // ----------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------

    public CommandRouter() {
        this(8);           // default: 8 shared database permits
    }

    public CommandRouter(int dbPermitCount) {

        dbPermits = new Semaphore(dbPermitCount, true); // fair

        // STRING gets the most partitions — it's the hottest path.
        partitions.put(DataType.STRING,
                new DataTypePartition(DataType.STRING, 4, 16, dbPermits));

        partitions.put(DataType.LIST,
                new DataTypePartition(DataType.LIST,   2, 16, dbPermits));

        partitions.put(DataType.HASH,
                new DataTypePartition(DataType.HASH,   2, 16, dbPermits));

        partitions.put(DataType.SET,
                new DataTypePartition(DataType.SET,    2, 16, dbPermits));

        partitions.put(DataType.ZSET,
                new DataTypePartition(DataType.ZSET,   2, 16, dbPermits));
    }

    // ----------------------------------------------------------------
    // Routing — called by RedisClientHandler for each command
    // ----------------------------------------------------------------

    /**
     * Route a command task to the correct partition based on
     * the command verb and the Redis key.
     *
     * @param commandVerb  e.g. "SET", "LPUSH", "ZADD"
     * @param key          Redis key (used for intra-partition ordering)
     * @param task         the work to execute (captures response channel)
     * @param timeout      max wait for queue space
     * @param unit         time unit of timeout
     * @throws TaskRejectedException if the target partition is full/shutting down
     * @throws InterruptedException  if the calling thread is interrupted
     */
    public void route(String commandVerb, String key, Runnable task,
                      long timeout, TimeUnit unit)
            throws InterruptedException {

        DataType type = classify(commandVerb);
        DataTypePartition partition = partitions.get(type);
        partition.submit(key, task, timeout, unit);
    }

    // ----------------------------------------------------------------
    // Graceful shutdown — drain all partitions
    // ----------------------------------------------------------------

    /**
     * Shut down all partitions in parallel, then wait for each to finish.
     *
     * Partitions are shut down concurrently (separate threads) so the
     * total wait is bounded by the slowest partition, not the sum of all.
     */
    public void shutdown() throws InterruptedException {

        // Signal all partitions simultaneously before joining any of them.
        Thread[] shutdownHelpers =
                new Thread[partitions.size()];

        int i = 0;
        for (DataTypePartition p : partitions.values()) {
            Thread t = new Thread(() -> {
                try {
                    p.shutdown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "shutdown-helper-" + p.getDataType());
            shutdownHelpers[i++] = t;
            t.start();
        }

        for (Thread t : shutdownHelpers) {
            t.join();
        }
    }

    // ----------------------------------------------------------------
    // Metrics
    // ----------------------------------------------------------------

    /**
     * Returns the partition for a given DataType.
     * Callers (monitor, stress test) can call partition.getTotalSubmitted()
     * etc. directly.
     */
    public DataTypePartition getPartition(DataType type) {
        return partitions.get(type);
    }

    /** Available database permits right now. */
    public int availableDbPermits() {
        return dbPermits.availablePermits();
    }

    /** Total workers across all partitions. */
    public int totalWorkers() {
        return partitions.values().stream()
                .mapToInt(DataTypePartition::getPartitionCount)
                .sum();
    }

    /** Prints a per-partition summary to stdout. */
    public void printMetrics() {

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.printf( "║  COMMAND ROUTER   dbPermits=%d available=%d%n",
                dbPermits.availablePermits() + dbPermits.getQueueLength(),
                dbPermits.availablePermits());
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.printf( "║  %-8s  %-6s  %-10s  %-10s  %-10s%n",
                "TYPE", "PARTS", "SUBMITTED", "COMPLETED", "REJECTED");
        System.out.println("╠══════════════════════════════════════════════════════╣");

        for (Map.Entry<DataType, DataTypePartition> entry : partitions.entrySet()) {
            DataTypePartition p = entry.getValue();
            System.out.printf(
                    "║  %-8s  %-6d  %-10d  %-10d  %-10d%n",
                    entry.getKey(),
                    p.getPartitionCount(),
                    p.getTotalSubmitted(),
                    p.getTotalCompleted(),
                    p.getTotalRejected()
            );
        }

        System.out.println("╠══════════════════════════════════════════════════════╣");

        for (Map.Entry<DataType, DataTypePartition> entry : partitions.entrySet()) {
            DataTypePartition p = entry.getValue();
            int[] depths = p.getQueueDepths();
            System.out.printf("║  %-6s queue depths: ", entry.getKey());
            for (int i = 0; i < depths.length; i++) {
                System.out.printf("[%d]=%d ", i, depths[i]);
            }
            System.out.println();
        }

        System.out.println("╠══════════════════════════════════════════════════════╣");

        for (Map.Entry<DataType, DataTypePartition> entry : partitions.entrySet()) {
            DataTypePartition p = entry.getValue();
            System.out.printf(
                    "║  %-6s  prodWait=%.3fms  consWait=%.3fms%n",
                    entry.getKey(),
                    p.avgProducerWaitMs(),
                    p.avgConsumerWaitMs()
            );
        }

        System.out.println("╚══════════════════════════════════════════════════════╝");
        System.out.println();
    }

    // ----------------------------------------------------------------
    // Internal — command verb → DataType classification
    // ----------------------------------------------------------------

    private static DataType classify(String verb) {

        return switch (verb.toUpperCase()) {

            // ---- String commands ----
            case "GET", "SET", "DEL", "INCR", "DECR",
                 "MGET", "MSET", "SETNX", "GETSET",
                 "APPEND", "STRLEN", "TTL", "PTTL",
                 "PERSIST", "EXPIRE", "PEXPIRE",
                 "GETDEL", "GETEX"
                    -> DataType.STRING;

            // ---- List commands ----
            case "LPUSH", "RPUSH", "LPOP", "RPOP",
                 "LLEN", "LRANGE", "LINDEX", "LSET",
                 "LREM", "LINSERT", "LTRIM",
                 "BLPOP", "BRPOP"
                    -> DataType.LIST;

            // ---- Hash commands ----
            case "HSET", "HGET", "HDEL", "HGETALL",
                 "HMSET", "HMGET", "HKEYS", "HVALS",
                 "HLEN", "HEXISTS", "HINCRBY", "HINCRBYFLOAT"
                    -> DataType.HASH;

            // ---- Set commands ----
            case "SADD", "SREM", "SMEMBERS", "SISMEMBER",
                 "SCARD", "SUNION", "SINTER", "SDIFF",
                 "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
                 "SMOVE", "SRANDMEMBER", "SPOP"
                    -> DataType.SET;

            // ---- Sorted Set commands ----
            case "ZADD", "ZREM", "ZSCORE", "ZRANK",
                 "ZRANGE", "ZRANGEBYSCORE", "ZCARD",
                 "ZINCRBY", "ZREVRANK", "ZREVRANGE",
                 "ZRANGEBYLEX", "ZREMRANGEBYRANK",
                 "ZREMRANGEBYSCORE", "ZCOUNT", "ZLEXCOUNT"
                    -> DataType.ZSET;

            // ---- Unknown — default to STRING partition ----
            default -> DataType.STRING;
        };
    }
}
