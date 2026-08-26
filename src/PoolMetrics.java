/**
 * Immutable snapshot of worker-pool metrics at a point in time.
 *
 * Produced by RedisWorkerPool.metrics() and consumed by
 * RedisPoolMonitor.  Using a record keeps the snapshot value-typed
 * and thread-safe (no shared mutable state).
 *
 * Field notes
 * -----------
 * producerWaitNanos  – total nanos all submitters spent waiting for
 *                      queue space.  High value → queue is the bottleneck.
 * consumerWaitNanos  – total nanos all workers spent waiting for tasks.
 *                      High value → system is underutilised.
 * avgProducerWaitMs  – convenience: mean wait per blocked submit call.
 * avgConsumerWaitMs  – convenience: mean wait per idle worker cycle.
 */
public record PoolMetrics(

        // ---- sizing ----
        int  workerCount,
        int  queueCapacity,

        // ---- live state ----
        int  activeWorkers,
        int  queuedTasks,

        // ---- lifecycle counters ----
        long submittedTasks,
        long completedTasks,
        long rejectedTasks,

        // ---- wait metrics ----
        long producerWaitCount,
        long consumerWaitCount,
        long producerWaitNanos,
        long consumerWaitNanos,

        // ---- pool state ----
        boolean shutdownRequested,
        PoolHealth health

) {

    // ----------------------------------------------------------------
    // Derived helpers
    // ----------------------------------------------------------------

    /** Queue fill as a fraction 0.0 – 1.0. */
    public double queueFillRatio() {
        if (queueCapacity == 0) return 0.0;
        return (double) queuedTasks / queueCapacity;
    }

    /** Average millis a producer waited per blocked offer, or 0. */
    public double avgProducerWaitMs() {
        if (producerWaitCount == 0) return 0.0;
        return (producerWaitNanos / (double) producerWaitCount) / 1_000_000.0;
    }

    /** Average millis a consumer waited per idle cycle, or 0. */
    public double avgConsumerWaitMs() {
        if (consumerWaitCount == 0) return 0.0;
        return (consumerWaitNanos / (double) consumerWaitCount) / 1_000_000.0;
    }

    /** Tasks in-flight: submitted but not yet completed or rejected. */
    public long inFlight() {
        return submittedTasks - completedTasks - rejectedTasks;
    }

    // ----------------------------------------------------------------
    // Readable toString
    // ----------------------------------------------------------------

    @Override
    public String toString() {
        return String.format(
                "PoolMetrics{health=%-12s workers=%d active=%d " +
                "queue=%d/%d submitted=%d completed=%d rejected=%d " +
                "prodWait=%.2fms consWait=%.2fms shutdown=%b}",
                health,
                workerCount,
                activeWorkers,
                queuedTasks,
                queueCapacity,
                submittedTasks,
                completedTasks,
                rejectedTasks,
                avgProducerWaitMs(),
                avgConsumerWaitMs(),
                shutdownRequested
        );
    }
}
