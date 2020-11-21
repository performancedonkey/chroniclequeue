package simulation;

import algoAPI.AlgoAPI;
import com.lmax.disruptor.RingBuffer;
import disrupcher.EventHolder;
import org.apache.log4j.Logger;
import utils.NanoClock;

public class DisruptedAlgoApi<T> extends DelegatorAlgoApi implements Batcher<T> {
    private static final Logger log = Logger.getLogger(DisruptedAlgoApi.class);

    private final RingBuffer<EventHolder<T>> ringBuffer;

    public DisruptedAlgoApi(AlgoAPI nested, Logger log, RingBuffer<EventHolder<T>> ringBuffer) {
        super(nested, log);
        this.ringBuffer = ringBuffer;
    }

    long sequence, lastPublish;

    @Override
    public void pushNext(T next) {
        ringBuffer.publishEvent(this::set, next);
    }

    private void set(EventHolder<T> event, long sequence, T newEvent) {
        this.sequence = sequence;
        event.set(newEvent, sequence);
        this.lastPublish = NanoClock.getNanoTimeNow();
    }

}
