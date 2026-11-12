package com.redis.pool;

import java.util.List;

/**
 * Minimal surface that {@link RedisPoolMonitor} needs to print a
 * health snapshot for any worker pool.
 *
 * Both {@link ConnectionPool} and {@link RedisWorkerPool} implement
 * this interface so the monitor is not tied to either concrete type.
 */
public interface MonitorablePool {

    /** Point-in-time immutable metrics snapshot. */
    PoolMetrics metrics();

    /** Per-worker observable state — one entry per worker thread. */
    List<WorkerInfo> getWorkerInfos();
}
