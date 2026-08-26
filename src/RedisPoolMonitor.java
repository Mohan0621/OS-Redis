/**
 * Background thread that periodically prints a pool health snapshot.
 *
 * v2 changes
 * ----------
 * - Reads PoolMetrics (immutable snapshot) instead of individual getters.
 * - Prints PoolHealth interpretation alongside raw numbers.
 * - Shows per-worker state so stalled or failed workers are visible.
 * - Shows derived averages (producer wait, consumer wait) in milliseconds.
 */
public class RedisPoolMonitor implements Runnable {

    private final RedisWorkerPool pool;
    private final long intervalMs;
    private volatile boolean stopped = false;

    public RedisPoolMonitor(RedisWorkerPool pool) {
        this(pool, 2_000);
    }

    public RedisPoolMonitor(RedisWorkerPool pool, long intervalMs) {
        this.pool       = pool;
        this.intervalMs = intervalMs;
    }

    /** Signal this monitor to stop after the current sleep. */
    public void stop() {
        stopped = true;
    }

    @Override
    public void run() {

        while (!stopped && !Thread.currentThread().isInterrupted()) {

            try {
                Thread.sleep(intervalMs);
                printSnapshot();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void printSnapshot() {

        PoolMetrics m = pool.metrics();

        System.out.println();
        System.out.println("╔══════════════════════════════════════╗");
        System.out.printf( "║  POOL HEALTH : %-22s║%n", m.health());
        System.out.println("╠══════════════════════════════════════╣");
        System.out.printf( "║  Workers     : %-5d                 ║%n", m.workerCount());
        System.out.printf( "║  Active      : %-5d                 ║%n", m.activeWorkers());
        System.out.printf( "║  Queue       : %d/%-5d  (%.0f%%)       ║%n",
                m.queuedTasks(), m.queueCapacity(),
                m.queueFillRatio() * 100);
        System.out.println("╠══════════════════════════════════════╣");
        System.out.printf( "║  Submitted   : %-8d             ║%n", m.submittedTasks());
        System.out.printf( "║  Completed   : %-8d             ║%n", m.completedTasks());
        System.out.printf( "║  Rejected    : %-8d             ║%n", m.rejectedTasks());
        System.out.printf( "║  In-flight   : %-8d             ║%n", m.inFlight());
        System.out.println("╠══════════════════════════════════════╣");
        System.out.printf( "║  Prod wait   : %.3f ms (n=%d)%n",
                m.avgProducerWaitMs(), m.producerWaitCount());
        System.out.printf( "║  Cons wait   : %.3f ms (n=%d)%n",
                m.avgConsumerWaitMs(), m.consumerWaitCount());
        System.out.println("╠══════════════════════════════════════╣");

        // Per-worker state — makes stalled/failed workers immediately visible
        for (WorkerInfo w : pool.getWorkerInfos()) {
            String extra = "";
            if (w.getState() == WorkerState.RUNNING) {
                extra = " +" + w.currentTaskDurationMillis() + "ms";
            }
            System.out.printf("║  %-14s  %-12s%s%n",
                    w.getName(), w.getState(), extra);
        }

        System.out.println("╚══════════════════════════════════════╝");
        System.out.println();
    }
}
