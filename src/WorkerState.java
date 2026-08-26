/**
 * Lifecycle states for a single worker thread.
 *
 * Normal path:
 *
 *   CREATED → WAITING → RUNNING → WAITING → ... → STOPPING → STOPPED
 *
 * Failure path:
 *
 *   RUNNING → FAILED
 *
 * FAILED workers are observable by the monitor so we can detect
 * when the pool has silently lost capacity.
 */
public enum WorkerState {

    /** Thread created, not yet started or has not entered the loop yet. */
    CREATED,

    /** Blocked on queue.take() — waiting for a task. */
    WAITING,

    /** Actively executing a task. */
    RUNNING,

    /** Received shutdown signal, finishing up. */
    STOPPING,

    /** Exited the worker loop cleanly. */
    STOPPED,

    /** Exited the worker loop due to an unexpected Throwable. */
    FAILED
}
