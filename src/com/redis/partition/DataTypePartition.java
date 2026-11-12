package com.redis.partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import com.redis.pool.PartitionedQueue;
import com.redis.pool.WorkerInfo;
import com.redis.pool.WorkerState;
import com.redis.pool.TaskRejectedException;

/**
 * One vertical slice of the partitioned architecture.
 *
 * Owns:
 *   - A PartitionedQueue  (N bounded queues, one per partition)
 *   - N worker threads    (one per partition — guarantees per-key ordering)
 *   - N WorkerInfo objects (observable lifecycle state per worker)
 *   - A shared Semaphore  (cross-partition database concurrency cap)
 *
 * Design choices
 * --------------
 * Worker-to-partition is 1:1.  Worker 0 only drains queue 0, worker 1
 * only drains queue 1, etc.  This is the crucial property: because a
 * given key always hashes to the same partition, and that partition has
 * exactly one worker, operations on the same key are always sequential.
 *
 * The shared Semaphore is injected from CommandRouter so the total
 * number of concurrent database operations across *all* data types is
 * bounded.  The semaphore is acquired before task.run() and released
 * in finally — the pattern used in RedisWorkerPool for the same reason.
 *
 * Metrics exposed
 * ---------------
 * getTotalSubmitted / getTotalCompleted / getTotalRejected
 * getQueueDepths()         — per-partition depth array
 * getTotalQueueDepth()     — sum across partitions
 * getWorkerInfos()         — observable worker lifecycle
 * getProducerWaitCount/Nanos — how long submitters waited for space
 */
public class DataTypePartition {

    // sentinel — same poison-pill trick as RedisWorkerPool
    private static final Runnable POISON_PILL = () -> {};

    private final DataType       dataType;
    private final PartitionedQueue queue;
    private final Semaphore      dbPermits;    // shared across all partitions
    private final int            partitionCount;

    private final List<Thread>     workerThreads;
    private final List<WorkerInfo> workerInfos;

    private final AtomicLong submittedTasks  = new AtomicLong();
    private final AtomicLong completedTasks  = new AtomicLong();
    private final AtomicLong rejectedTasks   = new AtomicLong();
    private final AtomicLong producerWaitCount = new AtomicLong();
    private final AtomicLong producerWaitNanos  = new AtomicLong();

    private volatile boolean shutdownRequested = false;

    // ----------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------

    /**
     * @param dataType             which Redis data structure this serves
     * @param partitionCount       number of independent queue+worker pairs
     * @param capacityPerPartition max tasks queued per partition
     * @param dbPermits            shared semaphore limiting concurrent DB ops
     */
    public DataTypePartition(
            DataType  dataType,
            int       partitionCount,
            int       capacityPerPartition,
            Semaphore dbPermits) {

        this.dataType       = dataType;
        this.partitionCount = partitionCount;
        this.dbPermits      = dbPermits;
        this.queue          = new PartitionedQueue(
                partitionCount, capacityPerPartition);

        List<Thread>     threads = new ArrayList<>(partitionCount);
        List<WorkerInfo> infos   = new ArrayList<>(partitionCount);

        for (int i = 0; i < partitionCount; i++) {

            // Name pattern: "string-worker-0", "list-worker-1", etc.
            String name = dataType.name().toLowerCase() + "-worker-" + i;

            WorkerInfo info    = new WorkerInfo(name);
            final int  partIdx = i;          // effectively final for lambda

            Thread thread = new Thread(
                    () -> workerLoop(info, partIdx),
                    name
            );
            thread.setDaemon(false);

            infos.add(info);
            threads.add(thread);
            thread.start();
        }

        this.workerThreads = Collections.unmodifiableList(threads);
        this.workerInfos   = Collections.unmodifiableList(infos);
    }

    // ----------------------------------------------------------------
    // Public API — called by CommandRouter
    // ----------------------------------------------------------------

    /**
     * Route a task for the given Redis key into the correct partition.
     *
     * The same key always maps to the same partition, preserving order
     * for operations on that key while allowing other keys to proceed
     * concurrently on different partitions.
     *
     * @param key     Redis key — used for partition selection only
     * @param task    work to run (captures the command + response channel)
     * @param timeout max wait for queue space
     * @param unit    time unit of timeout
     * @throws TaskRejectedException if pool is shutting down or queue full
     * @throws InterruptedException  if calling thread is interrupted
     */
    public void submit(String key, Runnable task,
                       long timeout, TimeUnit unit)
            throws InterruptedException {

        if (shutdownRequested) {
            rejectedTasks.incrementAndGet();
            throw new TaskRejectedException(
                    dataType + " partition is shutting down");
        }

        submittedTasks.incrementAndGet();

        long    start    = System.nanoTime();
        boolean accepted = queue.offer(key, task, timeout, unit);
        long    elapsed  = System.nanoTime() - start;

        if (elapsed > 0) {
            producerWaitCount.incrementAndGet();
            producerWaitNanos.addAndGet(elapsed);
        }

        if (!accepted) {
            rejectedTasks.incrementAndGet();
            throw new TaskRejectedException(
                    dataType + " queue[" + queue.partitionIndex(key)
                    + "] is full after "
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
     * Each worker drains its partition's real tasks before reaching
     * its pill — that is the "graceful" part.
     */
    public void shutdown() throws InterruptedException {

        shutdownRequested = true;

        // One pill per worker, injected into the worker's own partition.
        for (int i = 0; i < partitionCount; i++) {
            queue.putPoisonPill(i, POISON_PILL);
        }

        for (Thread t : workerThreads) {
            t.join();
        }
    }

    // ----------------------------------------------------------------
    // Metrics
    // ----------------------------------------------------------------

    public DataType  getDataType()           { return dataType;                    }
    public int       getPartitionCount()     { return partitionCount;              }
    public long      getTotalSubmitted()     { return submittedTasks.get();        }
    public long      getTotalCompleted()     { return completedTasks.get();        }
    public long      getTotalRejected()      { return rejectedTasks.get();         }
    public long      getProducerWaitCount()  { return producerWaitCount.get();     }
    public long      getProducerWaitNanos()  { return producerWaitNanos.get();     }
    public long      getConsumerWaitCount()  { return queue.getConsumerWaitCount();}
    public long      getConsumerWaitNanos()  { return queue.getConsumerWaitNanos();}
    public int       getTotalQueueDepth()    { return queue.getTotalDepth();       }
    public boolean   isShutdown()            { return shutdownRequested;           }

    /** Per-partition queue depths — index matches worker index. */
    public int[] getQueueDepths() {
        int[] depths = new int[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            depths[i] = queue.getDepth(i);
        }
        return depths;
    }

    public List<WorkerInfo> getWorkerInfos() {
        return workerInfos;
    }

    /** Average producer wait per blocked submit(), in milliseconds. */
    public double avgProducerWaitMs() {
        long count = producerWaitCount.get();
        if (count == 0) return 0.0;
        return (producerWaitNanos.get() / (double) count) / 1_000_000.0;
    }

    /** Average consumer wait per idle worker cycle, in milliseconds. */
    public double avgConsumerWaitMs() {
        long count = queue.getConsumerWaitCount();
        if (count == 0) return 0.0;
        return (queue.getConsumerWaitNanos() / (double) count) / 1_000_000.0;
    }

    // ----------------------------------------------------------------
    // Internal — worker loop
    // ----------------------------------------------------------------

    private void workerLoop(WorkerInfo info, int partitionIndex) {

        info.setState(WorkerState.WAITING);

        while (true) {

            try {

                // Block until a task arrives in *this worker's* partition.
                Runnable task = queue.take(partitionIndex);

                if (task == POISON_PILL) {
                    info.setState(WorkerState.STOPPING);
                    break;
                }

                // Acquire a database permit from the shared semaphore.
                // This gates how many workers across ALL data types can
                // be touching the data store simultaneously.
                dbPermits.acquire();

                info.setState(WorkerState.RUNNING);
                info.setLastTaskStartTime(System.currentTimeMillis());

                try {
                    task.run();

                } catch (Throwable t) {
                    info.setState(WorkerState.FAILED);
                    System.err.println(
                            "[" + info.getName() + "] task threw: "
                            + t.getClass().getSimpleName()
                            + ": " + t.getMessage());
                    // Worker survives task failure — same philosophy as pool v2.

                } finally {
                    dbPermits.release();
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
}
