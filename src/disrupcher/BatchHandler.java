package disrupcher;

import algoAPI.AlgoAPI;
import events.LiveEvent;
import events.book.BookAtom;
import events.book.LeanQuote;

import java.util.concurrent.atomic.AtomicInteger;

public class BatchHandler<T> extends EarlyReleaseHandler<EventHolder<T>> {

    long assimilated = 0;

    private final LiveEvent[] batch;
    private final AlgoAPI manager;

    public BatchHandler(AlgoAPI manager, int maxSize) {
        this.manager = manager;
        batch = new LiveEvent[maxSize];
    }

    private int batchSize = 0;

    AtomicInteger batchId = new AtomicInteger(0);

    @Override
    public void onEvent(EventHolder<T> eventHolder, long sequence, boolean endOfBatch) {
//        super.onEvent(eventHolder, sequence, endOfBatch);
        onEvent(eventHolder);
        batchSize++;
        assimilated++;

        // All private events are end of batches
        if (endOfBatch || shouldDispatch(eventHolder)) {
            dispatch();
        }
//        eventHolder.clear();
    }

    // I dont like this unsafe casting...
    private boolean shouldDispatch(EventHolder<T> eventHolder) {
        BookAtom event = (BookAtom) eventHolder.getEvent();
        return batchSize == batch.length - 1 ||
                event.getType().isPrivate() || ((LeanQuote) event).isLast();
    }

    @Override
    protected void onEvent(EventHolder<T> eventHolder) {
        batch[batchSize] = (LiveEvent) eventHolder.getEvent();
    }

    private synchronized void dispatch() {
        manager.pushBatch(batchId.get(), batch, batchSize);
        // reset batch
        batchSize = 0;
        batchId.incrementAndGet();
    }

    public void reset() {
        batchSize = 0;
        batchId.set(0);
    }

    public int getBatches() {
        return batchId.get();
    }
}
