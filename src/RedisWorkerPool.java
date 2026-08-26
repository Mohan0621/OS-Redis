import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Production-ish worker pool v2.
 *
 * What's new versus v1
 * --------------------
 * 1. WorkerInfo + WorkerState  — every thread exposes its lifecycle state.
 * 2. Fair semaphore            — executionPermits(fair=true) prevents barging.
 * 3. Timed submit()            — uses offer(timeout) instead of blocking put().
 * 4. TaskRejectedException     — returned when offer times out or pool is
 *                                already shutting down.
 * 5. Producer wait metrics     — records how long each submit call waited.
 * 6. Graceful shutdown         — poison-pill approach; workers drain the queue
 *                                before exiting, then join() confirms clean stop.
 * 7. Shutdown race guard       — submit() checks shutdownRequested before
 *                                touching the queue.
 * 8. metrics()                 — returns an immutable PoolMetrics snapshot
 *                                that the monitor (or any observer) can read.
 *
 * Shutdown state machine
 * ----------------------
 *   RUNNING → shutdown() called → SHUTTING_DOWN (shutdownRequested = true)
 *     → poison pills drained by workers → all workers STOPPED → TERMINATED
 *
 * Worker lifecycle per thread
 * ---------------------------
 *   CREATED → WAITING (blocking on take)
 *           → RUNNING (executing task)
 *           → WAITING → RUNNING → ...
 *           → STOPPING (interrupt or poison pill seen)
 *           → STOPPED
 *   At any RUNNING moment → FAILED if task throws Throwable
 *     (worker continues loop — it doesn't die on task failure)
 */
public class RedisWorkerPool {

    // ----------------------------------------------------------------
    // Sentinel used as a "poison pill" during shutdown.
    // Workers recognise it by reference identity (==), not .equals().
    // ----------------------------------------------------------------
    private static final Runnable POISON_PILL = () -> {};

    // ----------------------------------------------------------------
    // Core infrastructure
    // ----------------------------------------------------------------

    private final BoundedTaskQueue<Runnable> queue;

    /**
     * Fair semaphore: threads acquire permits in the order they arrived.
     * This prevents a lucky thread from repeatedly barging past waiters.
     * Trade-off: slightly lower raw throughput for more predictable latency.
     */
    private final Semaphore executionPermits;

    // ----------------------------------------------------------------
    // Worker registry
    // ----------------------------------------------------------------

    /** Immutable after construction — safe to read without locks. */
    private final List<Thread>     workerThreads;
    private final List<WorkerInfo> workerInfos;

    // ----------------------------------------------------------------
    // Counters
    // ----------------------------------------------------------------

    private final AtomicInteger activeWorkers  = new AtomicInteger();
    private final AtomicLong    submittedTasks = new AtomicLong();
    private final AtomicLong    completedTasks = new AtomicLong();
    private final AtomicLong    rejectedTasks  = new AtomicLong();

    // Producer-side wait: how long callers of submit() waited for queue space.
    private final AtomicLong producerWaitCount = new AtomicLong();
    private final AtomicLong producerWaitNanos  = new AtomicLong();

    // ----------------------------------------------------------------
    // Shutdown flag — volatile for cross-thread visibility without a lock
    // ----------------------------------------------------------------
    private volatile boolean shutdownRequested = false;

    // ----------------------------------------------------------------
    // Constructor
    // ----------------------------------------------------------------

    public RedisWorkerPool(int workerCount, int queueCapacity) {

        if (workerCount <= 0) {
            throw new IllegalArgumentException(
                    "workerCount must be positive, got: " + workerCount);
        }

        queue            = new BoundedTaskQueue<>(queueCapacity);
        executionPermits = new Semaphore(workerCount, true); // fair = true

        List<Thread>     threads = new ArrayList<>(workerCount);
        List<WorkerInfo> infos   = new ArrayList<>(workerCount);

        for (int i = 0; i < workerCount; i++) {

            WorkerInfo info   = new WorkerInfo("redis-worker-" + i);
            Thread     thread = new Thread(
                    () -> workerLoop(info),
                    info.getName()
            );

            thread.setDaemon(false); // must not die before shutdown() drains
            infos.add(info);
            threads.add(thread);
            thread.start();
        }

        workerThreads = Collections.unmodifiableList(threads);
        workerInfos   = Collections.unmodifiableList(infos);
    }

    // ----------------------------------------------------------------
    // Public API — producer side
    // ----------------------------------------------------------------

    /**
     * Submit a task with a bounded wait for queue space.
     *
     * Flow:
     *   pool shutting down  →  TaskRejectedException (shutdown)
     *   offer succeeds      →  task queued, return
     *   offer times out     →  TaskRejectedException (overloaded)
     *
     * Producer wait is measured regardless of outcome so we can see
     * how long producers are being held back by a saturated queue.
     *
     * @param task    work to execute
     * @param timeout max time to wait for queue space
     * @param unit    time unit of timeout
     * @throws TaskRejectedException  if pool is shutting down or overloaded
     * @throws InterruptedException   if the calling thread is interrupted
     */
    public void submit(Runnable task, long timeout, TimeUnit unit)
            throws InterruptedException {

        if (shutdownRequested) {
            rejectedTasks.incrementAndGet();
            throw new TaskRejectedException(
                    "Worker pool is shutting down — no new tasks accepted");
        }

        submittedTasks.incrementAndGet();

        long start    = System.nanoTime();
        boolean accepted = queue.offer(task, timeout, unit);
        long elapsed  = System.nanoTime() - start;

        // Record producer wait even when we're not blocked — the call still
        // consumed time, and accumulating zeros is cheap and honest.
        if (elapsed > 0) {
            producerWaitCount.incrementAndGet();
            producerWaitNanos.addAndGet(elapsed);
        }

        if (!accepted) {
            rejectedTasks.incrementAndGet();
            throw new TaskRejectedException(
                    "Worker pool is overloaded — queue full after "
                    + TimeUnit.NANOSECONDS.toMillis(elapsed) + " ms wait");
        }
    }

    // ----------------------------------------------------------------
    // Graceful shutdown
    // ----------------------------------------------------------------

    /**
     * Signal the pool to stop, wait for all in-flight and queued work
     * to finish, then return.
     *
     * Mechanism: one poison pill per worker thread is appended to the
     * queue.  Each worker, after draining any real tasks ahead of its
     * pill, sees the pill and exits its loop cleanly.
     *
     * Why this is graceful: tasks already in the queue when shutdown()
     * is called are completed before the workers stop.
     *
     * @throws InterruptedException if this thread is interrupted while
     *                              waiting for workers to finish
     */
    public void shutdown() throws InterruptedException {

        shutdownRequested = true;

        // One pill per worker ensures every worker wakes up and exits
        // even if multiple workers are blocked on take() simultaneously.
        for (int i = 0; i < workerThreads.size(); i++) {
            queue.put(POISON_PILL);
        }

        for (Thread t : workerThreads) {
            t.join();
        }
    }

    // ----------------------------------------------------------------
    // Metrics snapshot
    // ----------------------------------------------------------------

    /**
     * Returns an immutable point-in-time snapshot of pool state.
     * Safe to call from any thread at any time.
     */
    public PoolMetrics metrics() {

        int  qSize     = queue.size();
        int  qCap      = queue.capacity();
        int  active    = activeWorkers.get();
        long submitted = submittedTasks.get();
        long completed = completedTasks.get();
        long rejected  = rejectedTasks.get();
        long pwCount   = producerWaitCount.get();
        long cwCount   = queue.getConsumerWaitCount();
        long pwNanos   = producerWaitNanos.get();
        long cwNanos   = queue.getConsumerWaitNanos();

        PoolHealth health = deriveHealth(
                active, qSize, qCap, rejected, shutdownRequested);

        return new PoolMetrics(
                workerThreads.size(),
                qCap,
                active,
                qSize,
                submitted,
                completed,
                rejected,
                pwCount,
                cwCount,
                pwNanos,
                cwNanos,
                shutdownRequested,
                health
        );
    }

    /**
     * Returns an unmodifiable view of all WorkerInfo objects.
     * Useful for the monitor to print per-worker state.
     */
    public List<WorkerInfo> getWorkerInfos() {
        return workerInfos;
    }

    // ----------------------------------------------------------------
    // Internal — worker loop
    // ----------------------------------------------------------------

    private void workerLoop(WorkerInfo info) {

        info.setState(WorkerState.WAITING);

        while (true) {

            try {

                Runnable task = queue.take();

                // ---- Poison pill check ----
                if (task == POISON_PILL) {
                    info.setState(WorkerState.STOPPING);
                    break;
                }

                // ---- Acquire execution permit (fair semaphore) ----
                // This is the cross-worker concurrency gate. If all permits
                // are taken, this worker waits here — not on the queue.
                executionPermits.acquire();

                info.setState(WorkerState.RUNNING);
                info.setLastTaskStartTime(System.currentTimeMillis());
                activeWorkers.incrementAndGet();

                try {
                    task.run();

                } catch (Throwable t) {
                    // Task threw — mark the worker as FAILED momentarily
                    // so the monitor can detect it, then recover.
                    info.setState(WorkerState.FAILED);
                    System.err.println(
                            "[" + info.getName() + "] task threw: "
                            + t.getClass().getSimpleName()
                            + ": " + t.getMessage());
                    // Worker survives — it does NOT die on task failure.
                    // In a production pool you'd also increment a failedTasks
                    // counter and decide whether to restart the worker.

                } finally {
                    activeWorkers.decrementAndGet();
                    completedTasks.incrementAndGet();
                    executionPermits.release();

                    info.setLastTaskEndTime(System.currentTimeMillis());
                    info.setState(WorkerState.WAITING);
                }

            } catch (InterruptedException e) {
                // Interrupted externally (not via poison pill).
                info.setState(WorkerState.STOPPING);
                Thread.currentThread().interrupt();
                break;
            }
        }

        info.setState(WorkerState.STOPPED);
    }

    // ----------------------------------------------------------------
    // Internal — health derivation
    // ----------------------------------------------------------------

    private static PoolHealth deriveHealth(
            int active, int qSize, int qCap,
            long rejected, boolean shutdownRequested) {

        if (shutdownRequested) {
            // Check if all workers are done — if active == 0 and queue empty
            // we're effectively terminated, but we keep SHUTTING_DOWN here
            // because workers call setState(STOPPED) after the loop exits;
            // the monitor can check workerInfos for full termination.
            return PoolHealth.SHUTTING_DOWN;
        }

        double fill = (qCap == 0) ? 0.0 : (double) qSize / qCap;

        if (rejected > 0 && qSize == qCap) {
            return PoolHealth.OVERLOADED;
        }

        if (fill >= 0.80) {
            return PoolHealth.DEGRADED;
        }

        return PoolHealth.HEALTHY;
    }
}
