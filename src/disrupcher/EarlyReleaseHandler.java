package disrupcher;

import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceReportingEventHandler;

public abstract class EarlyReleaseHandler<T> implements SequenceReportingEventHandler<T> {
    private Sequence sequenceCallback;
    private int batchRemaining = 20;

    @Override
    public void setSequenceCallback(Sequence sequenceCallback) {
        this.sequenceCallback = sequenceCallback;
    }

    @Override
    public void onEvent(T event, long sequence, boolean endOfBatch) throws Exception {
        processEvent(event);

        boolean logicalChunkOfWorkComplete = isLogicalChunkOfWorkComplete();
        if (logicalChunkOfWorkComplete) {
            sequenceCallback.set(sequence);
        }

        batchRemaining = logicalChunkOfWorkComplete || endOfBatch ? 20 : batchRemaining;
    }

    private boolean isLogicalChunkOfWorkComplete() {
        // Ret true or false based on whatever cirteria is required for the smaller
        // chunk.  If this is doing I/O, it may be after flushing/syncing to disk
        // or at the end of DB batch+commit.
        // Or it could simply be working off a smaller batch size.

        return --batchRemaining == -1;
    }

    protected abstract void processEvent(T event);
}
