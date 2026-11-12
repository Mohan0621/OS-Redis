package com.redis.pool;

/**
 * Derived health status for a worker pool.
 *
 * This is computed from PoolMetrics — it is not a raw counter but
 * an interpretation that a monitoring system or dashboard can use
 * without understanding the underlying numbers.
 *
 * Transitions:
 *
 *   HEALTHY
 *     → DEGRADED      when queue fill >= 80 %
 *     → OVERLOADED    when queue full AND rejections are climbing
 *     → SHUTTING_DOWN when shutdown has been requested
 *     → TERMINATED    when all workers have stopped
 */
public enum PoolHealth {

    /** Active workers available, queue comfortably below 80 %. */
    HEALTHY,

    /** Queue is between 80 % and 100 % full — approaching saturation. */
    DEGRADED,

    /** Queue full, tasks are being rejected. */
    OVERLOADED,

    /** Shutdown requested; draining remaining queued tasks. */
    SHUTTING_DOWN,

    /** All workers have stopped; pool is no longer usable. */
    TERMINATED
}
