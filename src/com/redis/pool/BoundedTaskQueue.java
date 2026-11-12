package com.redis.pool;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A hand-rolled bounded blocking queue backed by a circular deque.
 *
 * New in v2
 * ---------
 * - offer(task, timeout, unit)  — timed insert; returns false on timeout
 *   instead of blocking forever like put() did.  This is the backpressure
 *   hook: a producer that cannot get space within its budget backs off.
 *
 * - Consumer wait metrics  — every worker that blocks on an empty queue
 *   increments consumerWaitCount and accumulates consumerWaitNanos.
 *   High values here mean the system is underutilised (workers idling).
 *
 * put() is kept for internal use (poison-pill injection during shutdown)
 * where the caller genuinely must wait and knows the queue will drain.
 */
public class BoundedTaskQueue<T> {

    private final Deque<T> queue = new ArrayDeque<>();

    private final int capacity;

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull  = lock.newCondition();

    // ---- consumer wait metrics ----
    // These are read by the pool's metrics() without holding the queue lock,
    // so AtomicLong gives us safe cross-thread visibility for free.
    private final AtomicLong consumerWaitCount = new AtomicLong();
    private final AtomicLong consumerWaitNanos  = new AtomicLong();

    // ----------------------------------------------------------------
    // Constructor
    // ----------------------------------------------------------------

    public BoundedTaskQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException(
                    "capacity must be positive, got: " + capacity);
        }
        this.capacity = capacity;
    }

    // ----------------------------------------------------------------
    // Producers
    // ----------------------------------------------------------------

    /**
     * Insert a task, waiting indefinitely if the queue is full.
     *
     * Only use this when you own the timing — e.g. the shutdown path
     * inserting poison pills.  Normal producers should use offer().
     */
    public void put(T task) throws InterruptedException {

        lock.lockInterruptibly();

        try {
            while (queue.size() == capacity) {
                notFull.await();
            }
            queue.addLast(task);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Insert a task, waiting at most {@code timeout} for queue space.
     *
     * Returns {@code true} if the task was accepted, {@code false} if
     * the timeout elapsed before space became available.
     *
     * This is the primary entry point for external producers.
     * The caller decides the budget; the queue decides whether it fits.
     *
     * Flow:
     *
     *   queue has space  →  insert immediately  →  true
     *
     *   queue full
     *     └─ wait (up to timeout)
     *           ├─ space appears  →  insert  →  true
     *           └─ timeout        →  false
     */
    public boolean offer(T task, long timeout, TimeUnit unit)
            throws InterruptedException {

        long nanos = unit.toNanos(timeout);

        lock.lockInterruptibly();

        try {
            while (queue.size() == capacity) {
                if (nanos <= 0) {
                    return false;          // timeout — tell producer to back off
                }
                nanos = notFull.awaitNanos(nanos);
            }

            queue.addLast(task);
            notEmpty.signal();
            return true;

        } finally {
            lock.unlock();
        }
    }

    // ----------------------------------------------------------------
    // Consumers
    // ----------------------------------------------------------------

    /**
     * Remove and return the head task, blocking until one is available.
     *
     * Records how long this thread waited so the pool can expose
     * consumer-side wait metrics.
     */
    public T take() throws InterruptedException {

        lock.lockInterruptibly();

        try {
            if (queue.isEmpty()) {

                long start = System.nanoTime();

                while (queue.isEmpty()) {
                    notEmpty.await();
                }

                long waited = System.nanoTime() - start;
                // Only count meaningful waits (eliminates spurious-wake noise)
                if (waited > 0) {
                    consumerWaitCount.incrementAndGet();
                    consumerWaitNanos.addAndGet(waited);
                }
            }

            T task = queue.removeFirst();
            notFull.signal();
            return task;

        } finally {
            lock.unlock();
        }
    }

    // ----------------------------------------------------------------
    // Introspection
    // ----------------------------------------------------------------

    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public int capacity() {
        return capacity;
    }

    public long getConsumerWaitCount() {
        return consumerWaitCount.get();
    }

    public long getConsumerWaitNanos() {
        return consumerWaitNanos.get();
    }
}
