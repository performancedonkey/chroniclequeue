package simulation;

import algoAPI.AlgoAPI;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import disrupcher.EventHolder;
import org.apache.log4j.Logger;
import utils.NanoClock;

public class AlgoDisruptor<T> extends AlgoDelegator<T>  {
    private static final Logger log = Logger.getLogger(AlgoDisruptor.class);

    private final RingBuffer<EventHolder<T>> ringBuffer;

    public AlgoDisruptor(AlgoAPI nested, Logger log, RingBuffer<EventHolder<T>> ringBuffer) {
        super(nested, log);
        this.ringBuffer = ringBuffer;
    }

    private final EventTranslatorOneArg<EventHolder<T>, T> translator = AlgoDisruptor.this::set;

    @Override
    public void pushNext(T next, boolean isLast) {
        ringBuffer.publishEvent(translator, next);
    }

    long lastPublish;

    private void set(EventHolder<T> eventHolder, long sequence, T newEvent) {
        eventHolder.set(newEvent, sequence);
        this.lastPublish = NanoClock.getNanoTimeNow();
    }

}
