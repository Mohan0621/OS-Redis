package com.redis.pool;

/**
 * Thrown when a task cannot be accepted by the worker pool.
 *
 * Two causes:
 *   1. The queue is full and the timed offer() expired — the system
 *      is overloaded and cannot absorb more work right now.
 *   2. The pool is shutting down and no longer accepts new tasks.
 *
 * The caller should treat this as a signal to push back to the
 * upstream producer (backpressure), not silently drop the work.
 */
public class TaskRejectedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TaskRejectedException(String message) {
        super(message);
    }
}
