package simulation;

import algoAPI.AlgoAPI;
import disrupcher.BatchHandler;
import disrupcher.EventHolder;
import events.book.BookAtom;
import org.apache.log4j.Logger;
import utils.NanoClock;

import java.util.concurrent.atomic.AtomicLong;

public class AlgoBatcher<T> extends AlgoDelegator<T> {
    private static final Logger log = Logger.getLogger(AlgoBatcher.class);
    BatchHandler<T> batcher;

    public AlgoBatcher(AlgoAPI nested, Logger log) {
        super(nested, log);
        batcher = new BatchHandler<>(this);
    }

    private final EventHolder<T> dummy = new EventHolder<>();

    private AtomicLong sequence = new AtomicLong(0);

    @Override
    public void pushNext(T next, boolean isLast) {
        long sequence = this.sequence.incrementAndGet();
        batcher.onEvent(set(dummy, sequence, next), sequence, isLast);
    }

    long lastPublish;

    private EventHolder<T> set(EventHolder<T> eventHolder, long sequence, T newEvent) {
        eventHolder.set(newEvent, sequence);
        this.lastPublish = NanoClock.getNanoTimeNow();
        return eventHolder;
    }

    @Override
    public void reset() {
        super.reset();
        lastPublish = 0;
        sequence.set(0l);
    }

}
