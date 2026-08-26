/**
 * Observable state for a single worker thread.
 *
 * All mutable fields are volatile so the monitor thread can read
 * them without holding any lock — a stale read here is acceptable
 * because this is observability data, not coordination data.
 */
public class WorkerInfo {

    private final String name;

    private volatile WorkerState state;

    /** Wall-clock millis when the current (or last) task started. */
    private volatile long lastTaskStartTime;

    /** Wall-clock millis when the last task finished. */
    private volatile long lastTaskEndTime;

    public WorkerInfo(String name) {
        this.name  = name;
        this.state = WorkerState.CREATED;
    }

    // ----------------------------------------------------------------
    // Accessors
    // ----------------------------------------------------------------

    public String getName() {
        return name;
    }

    public WorkerState getState() {
        return state;
    }

    public void setState(WorkerState state) {
        this.state = state;
    }

    public long getLastTaskStartTime() {
        return lastTaskStartTime;
    }

    public void setLastTaskStartTime(long time) {
        this.lastTaskStartTime = time;
    }

    public long getLastTaskEndTime() {
        return lastTaskEndTime;
    }

    public void setLastTaskEndTime(long time) {
        this.lastTaskEndTime = time;
    }

    /**
     * Milliseconds the worker has been running its current task.
     * Returns 0 if the worker is not in RUNNING state.
     */
    public long currentTaskDurationMillis() {
        if (state == WorkerState.RUNNING && lastTaskStartTime > 0) {
            return System.currentTimeMillis() - lastTaskStartTime;
        }
        return 0;
    }

    @Override
    public String toString() {
        return name + "[" + state + "]";
    }
}
