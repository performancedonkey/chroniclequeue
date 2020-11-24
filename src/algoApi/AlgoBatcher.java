package algoApi;

import algoAPI.AlgoAPI;
import disrupcher.BatchHandler;
import disrupcher.EventHolder;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

public class AlgoBatcher<T> extends AlgoDelegator<T> {
    private static final Logger log = Logger.getLogger(AlgoBatcher.class);
    BatchHandler<T> batchHandler;

    public AlgoBatcher(AlgoAPI nested, Logger log) {
        super(nested, log);
        batchHandler = new BatchHandler<>(this, 256);
    }

    private final EventHolder<T> dummy = new EventHolder<>();

    private AtomicLong sequence = new AtomicLong(0);

    @Override
    public void pushNext(T next, boolean isLast) {
        long sequence = this.sequence.incrementAndGet();
        batchHandler.onEvent(set(dummy, sequence, next), sequence, isLast);
    }

    private EventHolder<T> set(EventHolder<T> eventHolder, long sequence, T newEvent) {
        eventHolder.set(newEvent, sequence);
        return eventHolder;
    }

    @Override
    public void reset() {
        super.reset();
        sequence.set(0l);
    }

}
