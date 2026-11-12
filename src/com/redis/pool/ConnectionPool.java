package com.redis.pool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import com.redis.db.CommandHandler;
import com.redis.db.RedisDatabase;
import com.redis.partition.CommandRouter;
import com.redis.server.RedisClientHandler;

/**
 * Custom connection-handling pool.
 *
 * Responsibility
 * --------------
 * Accept {@link ConnectionTask}s (each wrapping one TCP socket), queue
 * them, and dispatch them to a fixed set of worker threads that each
 * run a {@link RedisClientHandler} to completion.
 *
 * Deliberately no semaphore here
 * -------------------------------
 * {@link RedisWorkerPool} had an internal semaphore because it was the
 * only concurrency layer.  In our current architecture the semaphore
 * lives inside {@link CommandRouter} / DataTypePartition, where
 * it gates the actual database operations.  Connection workers must NOT
 * hold a DB permit — they just read bytes from the socket, parse RESP,
 * and hand off {@link Runnable} tasks to the partition queues.  Adding
 * a semaphore here would reintroduce the coupling we deliberately split.
 *
 * Worker lifecycle
 * ----------------
 *   CREATED → WAITING (blocking on queue.take())
 *           → RUNNING (handler.run() in progress)
 *           → WAITING → RUNNING → ...
 *           → STOPPING (poison pill seen or interrupt)
 *           → STOPPED
 *   RUNNING → FAILED  (handler threw unexpectedly; worker recovers)
 *
 * Shutdown
 * --------
 * Poison-pill approach: one sentinel per worker is appended to the
 * queue.  Workers drain any real tasks ahead of their pill, then exit.
 * The caller of shutdown() blocks on join() until every worker is done.
 *
 * Backpressure
 * ------------
 * submit() uses offer(timeout) rather than a blocking put().  If the
 * queue is full for the entire offer window the task is rejected with
 * {@link TaskRejectedException}.  The accept loop in
 * {@link com.redis.server.RedisServer} catches this and immediately writes an error
 * response to the client, so no connection ever leaks.
 */
public class ConnectionPool implements MonitorablePool {

    // ----------------------------------------------------------------
    // Poison pill — identity-compared, never executed
    // ----------------------------------------------------------------
    private static final ConnectionTask POISON_PILL =
            new ConnectionTask(null);

    // ----------------------------------------------------------------
    // Injected dependencies (set at construction, read by workers)
    // ----------------------------------------------------------------
    private final CommandHandler commandHandler;
    private final RedisDatabase  database;
    private final CommandRouter  router;

    // ----------------------------------------------------------------
    // Core infrastructure
    // ----------------------------------------------------------------
    private final BoundedTaskQueue<ConnectionTask> queue;

    // Immutable after construction
    private final List<Thread>     workerThreads;
    private final List<WorkerInfo> workerInfos;

    // ----------------------------------------------------------------
    // Counters
    // ----------------------------------------------------------------
    private final AtomicInteger activeWorkers  = new AtomicInteger();
    private final AtomicLong    submittedTasks = new AtomicLong();
    private final AtomicLong    completedTasks = new AtomicLong();
    private final AtomicLong    rejectedTasks  = new AtomicLong();

    private final AtomicLong producerWaitCount = new AtomicLong();
    private final AtomicLong producerWaitNanos  = new AtomicLong();

    // ----------------------------------------------------------------
    // Shutdown flag
    // ----------------------------------------------------------------
    private volatile boolean shutdownRequested = false;

    // ----------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------

    /**
     * @param workerCount    number of connection-handler threads (e.g. 4)
     * @param queueCapacity  max pending connections in the queue (e.g. 8)
     * @param commandHandler executes parsed Redis commands
     * @param database       needed for AOF persistence inside the handler
     * @param router         routes each command to the correct partition
     */
    public ConnectionPool(
            int          workerCount,
            int          queueCapacity,
            CommandHandler commandHandler,
            RedisDatabase  database,
            CommandRouter  router) {

        if (workerCount <= 0) {
            throw new IllegalArgumentException(
                    "workerCount must be positive, got: " + workerCount);
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException(
                    "queueCapacity must be positive, got: " + queueCapacity);
        }

        this.commandHandler = commandHandler;
        this.database       = database;
        this.router         = router;
        this.queue          = new BoundedTaskQueue<>(queueCapacity);

        List<Thread>     threads = new ArrayList<>(workerCount);
        List<WorkerInfo> infos   = new ArrayList<>(workerCount);

        for (int i = 0; i < workerCount; i++) {

            WorkerInfo info   = new WorkerInfo("connection-worker-" + i);
            Thread     thread = new Thread(
                    () -> workerLoop(info),
                    info.getName()
            );

            thread.setDaemon(false); // must not exit before shutdown() drains
            infos.add(info);
            threads.add(thread);
            thread.start();
        }

        workerThreads = Collections.unmodifiableList(threads);
        workerInfos   = Collections.unmodifiableList(infos);
    }

    // ----------------------------------------------------------------
    // Public API — producer side (called by the TCP accept loop)
    // ----------------------------------------------------------------

    /**
     * Queue a new connection for handling.
     *
     * Waits up to {@code timeout} for queue space.  If the queue
     * remains full the connection is rejected — the caller is
     * responsible for closing the socket and notifying the client.
     *
     * @throws TaskRejectedException if shutting down or queue full
     * @throws InterruptedException  if the accept thread is interrupted
     */
    public void submit(ConnectionTask task, long timeout, TimeUnit unit)
            throws InterruptedException {

        if (shutdownRequested) {
            rejectedTasks.incrementAndGet();
            throw new TaskRejectedException(
                    "ConnectionPool is shutting down — connection rejected");
        }

        submittedTasks.incrementAndGet();

        long    start    = System.nanoTime();
        boolean accepted = queue.offer(task, timeout, unit);
        long    elapsed  = System.nanoTime() - start;

        if (elapsed > 0) {
            producerWaitCount.incrementAndGet();
            producerWaitNanos.addAndGet(elapsed);
        }

        if (!accepted) {
            rejectedTasks.incrementAndGet();
            throw new TaskRejectedException(
                    "ConnectionPool queue full after "
                    + TimeUnit.NANOSECONDS.toMillis(elapsed) + " ms wait");
        }
    }

    // ----------------------------------------------------------------
    // Graceful shutdown
    // ----------------------------------------------------------------

    /**
     * Signal shutdown, inject one poison pill per worker, then block
     * until all workers have exited.
     *
     * Any connection tasks already queued are processed before the
     * workers stop.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    public void shutdown() throws InterruptedException {

        shutdownRequested = true;

        for (int i = 0; i < workerThreads.size(); i++) {
            queue.put(POISON_PILL);
        }

        for (Thread t : workerThreads) {
            t.join();
        }
    }

    // ----------------------------------------------------------------
    // Metrics
    // ----------------------------------------------------------------

    /**
     * Returns an immutable point-in-time snapshot of pool state.
     * Safe to call from any thread without holding any lock.
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
     * Returns an unmodifiable view of per-worker state objects.
     * Used by {@link RedisPoolMonitor} to print per-thread status.
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

                ConnectionTask task = queue.take();

                // Poison pill — time to exit cleanly
                if (task == POISON_PILL) {
                    info.setState(WorkerState.STOPPING);
                    break;
                }

                info.setState(WorkerState.RUNNING);
                info.setLastTaskStartTime(System.currentTimeMillis());
                activeWorkers.incrementAndGet();

                try {

                    handle(task);

                } catch (Throwable t) {

                    info.setState(WorkerState.FAILED);
                    System.err.println(
                            "[" + info.getName() + "] handler threw: "
                            + t.getClass().getSimpleName()
                            + ": " + t.getMessage());
                    // Worker survives — same philosophy as RedisWorkerPool.
                    // A bad client should not kill the worker thread.

                } finally {

                    activeWorkers.decrementAndGet();
                    completedTasks.incrementAndGet();
                    info.setLastTaskEndTime(System.currentTimeMillis());
                    info.setState(WorkerState.WAITING);
                }

            } catch (InterruptedException e) {
                info.setState(WorkerState.STOPPING);
                Thread.currentThread().interrupt();
                break;
            }
        }

        info.setState(WorkerState.STOPPED);
    }

    /**
     * Run the full client session for one accepted connection.
     *
     * RedisClientHandler.run() blocks this worker thread for the
     * duration of the client session (reading commands, submitting
     * them to the partition queues, waiting for results, writing
     * responses).  When the client disconnects, run() returns and
     * the socket is closed here in the finally block.
     *
     * The worker does NOT hold any database permit during this time —
     * the semaphore lives in DataTypePartition and is only held during
     * the actual database operation inside the partition consumer.
     */
    private void handle(ConnectionTask task) {

        try {

            new RedisClientHandler(
                    task.getSocket(),
                    commandHandler,
                    database,
                    router
            ).run();

        } finally {

            try {
                task.getSocket().close();
            } catch (IOException ignored) {}
        }
    }

    // ----------------------------------------------------------------
    // Internal — health derivation
    // ----------------------------------------------------------------

    private static PoolHealth deriveHealth(
            int active, int qSize, int qCap,
            long rejected, boolean shutdownRequested) {

        if (shutdownRequested)           return PoolHealth.SHUTTING_DOWN;
        if (rejected > 0 && qSize == qCap) return PoolHealth.OVERLOADED;
        if (qCap > 0 && (double) qSize / qCap >= 0.80) return PoolHealth.DEGRADED;
        return PoolHealth.HEALTHY;
    }
}
