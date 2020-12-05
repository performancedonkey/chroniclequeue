package algoApi;

import algoAPI.AlgoAPI;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import disrupcher.EventHolder;
import org.apache.log4j.Logger;

public class AlgoDisruptor<T> extends AlgoBatcher<T> {

    private final EventTranslatorOneArg<EventHolder<T>, T> translator = AlgoDisruptor.this::set;
    private final RingBuffer<EventHolder<T>> ringBuffer;

    public AlgoDisruptor(AlgoAbstract nested, Logger log, RingBuffer<EventHolder<T>> ringBuffer) {
        super(nested, log);
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void pushNext(T next, boolean isLast) {
        // TODO isLast?
        ringBuffer.publishEvent(translator, next);
    }

}
