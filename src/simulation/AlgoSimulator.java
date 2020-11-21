package simulation;

import algoAPI.AlgoAPI;
import algoAPI.AlgoResultCallback;
import com.lmax.disruptor.RingBuffer;
import disrupcher.EventHolder;
import events.book.BookAtom;
import org.apache.log4j.Logger;
import simulator.ExchangeSimulator;

public class AlgoSimulator extends DisruptedAlgoApi<BookAtom> {

    public AlgoSimulator(AlgoAPI nested, Logger log, RingBuffer<EventHolder<BookAtom>> ringBuffer) {
        super(nested, log, ringBuffer);
    }

    @Override
    public void pushNext(BookAtom next) {
        if (next.getType().isPrivate()) {

        }
        super.pushNext(next);
    }

//    @Override
//    public void initTradable(LiveEvent initializeTradableEvent) {
//        super.initTradable(initializeTradableEvent);
//        PrivateOrderBook privateBook = new PrivateOrderBook((InitializeTradableEvent) initializeTradableEvent);
//        new MatchingEngine(privateBook);
//    }

    @Override
    public void addCallbackListener(AlgoResultCallback algoResultCallback) {
        ExchangeSimulator exchange = new ExchangeSimulator();
        nested.addCallbackListener(algoResultCallback);
    }

}