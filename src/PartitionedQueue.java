import java.util.concurrent.TimeUnit;

/**
 * A set of N independent bounded queues that partition work by key hash.
 *
 * Why this exists
 * ---------------
 * A single queue serialises all work through one FIFO line.  Two LPUSH
 * operations on completely unrelated keys still wait behind each other.
 * With N partitions, keys hash to independent queues so unrelated keys
 * make progress concurrently while operations on the *same* key stay
 * ordered within their partition.
 *
 * Key-based routing
 * -----------------
 * partition = Math.abs(key.hashCode()) % partitionCount
 *
 * This is deterministic: the same key always lands on the same queue
 * and therefore the same worker.  That is the ordering guarantee.
 *
 * Metrics
 * -------
 * getDepth(i)          — current queue depth for partition i
 * getTotalDepth()      — sum across all partitions (total backlog)
 * getConsumerWaitCount/Nanos — aggregated from each sub-queue
 *
 * These let CommandRouter report per-partition backlog so we can see
 * which data-type queues are saturating under load.
 */
public class PartitionedQueue {

    private final BoundedTaskQueue<Runnable>[] partitions;
    private final int partitionCount;

    // ----------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public PartitionedQueue(int partitionCount, int capacityPerPartition) {

        if (partitionCount <= 0) {
            throw new IllegalArgumentException(
                    "partitionCount must be positive, got: " + partitionCount);
        }
        if (capacityPerPartition <= 0) {
            throw new IllegalArgumentException(
                    "capacityPerPartition must be positive, got: "
                    + capacityPerPartition);
        }

        this.partitionCount = partitionCount;
        this.partitions     = new BoundedTaskQueue[partitionCount];

        for (int i = 0; i < partitionCount; i++) {
            partitions[i] = new BoundedTaskQueue<>(capacityPerPartition);
        }
    }

    // ----------------------------------------------------------------
    // Routing — producers call these
    // ----------------------------------------------------------------

    /**
     * Route a task to the partition determined by {@code key}.
     *
     * Identical keys always map to the same partition, preserving
     * insertion order for operations on the same logical object.
     *
     * Returns {@code true} if the task was accepted within the timeout,
     * {@code false} if the target partition was full the whole time.
     */
    public boolean offer(String key, Runnable task,
                         long timeout, TimeUnit unit)
            throws InterruptedException {

        int idx = partitionIndex(key);
        return partitions[idx].offer(task, timeout, unit);
    }

    /**
     * Route a task using a pre-computed partition index.
     * Use when the caller has already chosen the partition explicitly
     * (e.g. a worker draining its own queue during shutdown).
     */
    public boolean offer(int partitionIndex, Runnable task,
                         long timeout, TimeUnit unit)
            throws InterruptedException {

        return partitions[partitionIndex].offer(task, timeout, unit);
    }

    // ----------------------------------------------------------------
    // Consumption — workers call these
    // ----------------------------------------------------------------

    /**
     * Block until a task is available on partition {@code index},
     * then remove and return it.
     */
    public Runnable take(int partitionIndex) throws InterruptedException {
        return partitions[partitionIndex].take();
    }

    // ----------------------------------------------------------------
    // Shutdown support
    // ----------------------------------------------------------------

    /**
     * Inject a poison pill into partition {@code index}.
     * Called by DataTypePartition.shutdown() — one pill per worker.
     */
    public void putPoisonPill(int partitionIndex, Runnable pill)
            throws InterruptedException {
        partitions[partitionIndex].put(pill);
    }

    // ----------------------------------------------------------------
    // Introspection
    // ----------------------------------------------------------------

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getCapacityPerPartition() {
        return partitions[0].capacity();
    }

    /** Current task count queued in partition {@code index}. */
    public int getDepth(int index) {
        return partitions[index].size();
    }

    /** Total tasks queued across all partitions. */
    public int getTotalDepth() {
        int total = 0;
        for (BoundedTaskQueue<Runnable> q : partitions) {
            total += q.size();
        }
        return total;
    }

    /** Aggregated consumer wait event count across all partitions. */
    public long getConsumerWaitCount() {
        long sum = 0;
        for (BoundedTaskQueue<Runnable> q : partitions) {
            sum += q.getConsumerWaitCount();
        }
        return sum;
    }

    /** Aggregated consumer wait nanos across all partitions. */
    public long getConsumerWaitNanos() {
        long sum = 0;
        for (BoundedTaskQueue<Runnable> q : partitions) {
            sum += q.getConsumerWaitNanos();
        }
        return sum;
    }

    // ----------------------------------------------------------------
    // Internal
    // ----------------------------------------------------------------

    /**
     * Maps a Redis key to a partition index.
     *
     * Math.abs guards against negative hashCode values.
     * Integer.MIN_VALUE has no positive counterpart, so we special-case it.
     */
    public int partitionIndex(String key) {
        int h = key.hashCode();
        if (h == Integer.MIN_VALUE) h = 0;
        return Math.abs(h) % partitionCount;
    }
}
