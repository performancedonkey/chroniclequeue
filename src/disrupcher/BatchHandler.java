package disrupcher;

import algoAPI.AlgoAPI;
import events.LiveEvent;
import events.book.BookAtom;
import events.book.LeanQuote;

import java.util.concurrent.atomic.AtomicInteger;

public class BatchHandler<T> extends EarlyReleaseHandler<EventHolder<T>> {

    long assimilated = 0;

    final AlgoAPI manager;

    public BatchHandler(AlgoAPI manager) {
        this.manager = manager;
    }

    private LiveEvent[] batch = new LiveEvent[256];
    private int batchSize = 0;

    AtomicInteger batchId = new AtomicInteger(0);

    @Override
    public void onEvent(EventHolder<T> eventHolder, long sequence, boolean endOfBatch) {
//        super.onEvent(eventHolder, sequence, endOfBatch);
        onEvent(eventHolder);
        batchSize++;
//        if (batchId.get() >= 7219 && batchId.get()<=7229)
//            System.out.println(DateUtils.formatDateTimeMicro(eventHolder.getEvent().getTimestamp()) +" @ " +batchSize +
//                    " :\t " + eventHolder + " / " + eventHolder.getEvent().getLayer());
        assimilated++;

        // All private events are end of batches
        if (shouldDispatch(eventHolder)) {
            dispatch();
        }
//        eventHolder.clear();
    }

    private boolean shouldDispatch(EventHolder<T> eventHolder) {
        return batchSize == batch.length - 1 ||
                ((LeanQuote) eventHolder.getEvent()).isLast();
    }

    @Override
    protected void onEvent(EventHolder<T> eventHolder) {
        batch[batchSize] = (LiveEvent) eventHolder.getEvent();
    }

    private synchronized void dispatch() {
        manager.pushBatch(batchId.get(), batch, batchSize);

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
