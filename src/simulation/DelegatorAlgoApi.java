package simulation;

import algoAPI.AlgoAPI;
import algoAPI.AlgoAction;
import algoAPI.AlgoOperation;
import algoAPI.AlgoResultCallback;
import events.LiveEvent;
import events.book.BookAtom;
import org.apache.log4j.Logger;

public class DelegatorAlgoApi extends AbstractAlgoApi {
    private final static Logger log = Logger.getLogger(AlgoBatcher.class);
    protected final AlgoAPI nested;

    public DelegatorAlgoApi(AlgoAPI nested, Logger log) {
        this.nested = nested;
//        this.log = log;
    }

    @Override
    public void initTradable(LiveEvent initializeTradableEvent) {
        nested.initTradable(initializeTradableEvent);
    }

    @Override
    public void pushBatch(long batchNumber, LiveEvent[] events, int batchSize) {
//        System.out.println("sendingbatchof " + batchSize + " starting @ " + ((BookAtom)events[0]).getTimestamps().getSequence());
        long start = System.nanoTime();
        nested.pushBatch(batchNumber, events, batchSize);
        totalPushed += batchSize;

        long procTime = System.nanoTime() - start;
//        log.info("Pushed #" + batchNumber + ":\t" + batchSize + "\t -> " + totalPushed + "\t us " + procTime / 1_000);
//        if (lastProcTime > 1_000_000) { // 1 ms
//            if (isLive)
//                log.warn(batchNumber + " High process time: " + (lastProcTime / 1_000_000) + " ms for " + batchNumber + " events");
//        }
    }

    public long getPushed() {
        return totalPushed;
    }


    protected long totalPushed = 0;

    @Override
    public void done() {
        nested.done();
    }

    @Override
    public void addCallbackListener(AlgoResultCallback callbackListener) {
        nested.addCallbackListener(callbackListener);
    }

    @Override
    public void activate(String instrumentId, AlgoAction action, AlgoOperation operation) {
        nested.activate(instrumentId, action, operation);
    }

    @Override
    public void disable(String instrumentId, AlgoAction action, AlgoOperation operation) {
        nested.disable(instrumentId, action, operation);
    }

    @Override
    public void setProperty(String property, String value) {
        nested.setProperty(property, value);
    }

    public AlgoAPI getNested() {
        return nested;
    }

    public boolean isBusy() {
        return false;
    }
}
