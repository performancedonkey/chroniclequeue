package simulation;

import algoAPI.AlgoAPI;
import algoAPI.AlgoAction;
import algoAPI.AlgoOperation;
import algoAPI.AlgoResultCallback;
import events.LiveEvent;
import org.apache.log4j.Logger;

public abstract class AlgoDelegator<T> extends AlgoAbstract implements Batcher<T> {
    private final static Logger log = Logger.getLogger(AlgoDelegator.class);
    protected final AlgoAPI nested;

    public AlgoDelegator(AlgoAPI nested, Logger log) {
        this.nested = nested;
//        this.log = log;
    }

    @Override
    public void initTradable(LiveEvent initializeTradableEvent) {
        nested.initTradable(initializeTradableEvent);
    }

    public int longestBatch;
    public long longestBatchId;

    @Override
    public void pushBatch(long batchNumber, LiveEvent[] events, int batchSize) {
        long start = System.nanoTime();
        nested.pushBatch(batchNumber, events, batchSize);
        totalPushed += batchSize;
        batches++;
        if (batchSize > longestBatch) {
            longestBatch = batchSize;
            longestBatchId = batchNumber;
//                if (batchSize > 160)
//                    System.out.println("new longest #" + batchId + ": " + batchSize);
        }

//        long procTime = System.nanoTime() - start;
//        log.info("Pushed #" + batchNumber + ":\t" + batchSize + "\t -> " + totalPushed + "\t us " + procTime / 1_000);
//        if (procTime > 1_000_000) { // 1 ms
//            if (isLive)
//                log.warn(batchNumber + " High process time: " + (procTime / 1_000_000) + " ms for " + batchNumber + " events");
//        }
    }

    public long getPushed() {
        return totalPushed;
    }

    protected int batches = 0;
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

    public void reset() {
        totalPushed = 0;
        longestBatch = 0;
        batches = 0;
    }

    public int getLongestBatch() {
        return longestBatch;
    }

    public int getBatches() {

        return batches;
    }
}
