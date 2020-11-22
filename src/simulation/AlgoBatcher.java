package simulation;

import algoAPI.AlgoAPI;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import disrupcher.BatchHandler;
import disrupcher.EventHolder;
import org.apache.log4j.Logger;
import utils.NanoClock;

import java.util.concurrent.atomic.AtomicLong;

public class AlgoBatcher<T> extends AlgoDelegator<T> {
    private static final Logger log = Logger.getLogger(AlgoBatcher.class);
    BatchHandler batcher;

    public AlgoBatcher(AlgoAPI nested, Logger log) {
        super(nested, log);
        batcher = new BatchHandler(this);
    }

    private final EventHolder<T> dummy = new EventHolder<>();

    AtomicLong sequence = new AtomicLong(0);

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

}
