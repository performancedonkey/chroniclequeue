package algoApi;

import algoAPI.AlgoAPI;
import algoAPI.AlgoAction;
import algoAPI.AlgoOperation;
import algoAPI.AlgoResultCallback;
import events.LiveEvent;
import events.feed.InitializeTradableEvent;
import org.apache.log4j.Logger;
import trackers.PrivateOrderBook;
import utils.NanoClock;

public abstract class AlgoDelegator<T> extends AlgoAbstract implements Batcher<T> {
    protected final static Logger log = Logger.getLogger(AlgoDelegator.class);
    protected final AlgoAbstract nested;

    public AlgoDelegator(AlgoAbstract nested, Logger log) {
        this.nested = nested;
//        this.log = log;
    }

    @Override
    public void initTradable(LiveEvent initEvent) {
        nested.initTradable(initEvent);
    }

    @Override
    public InitializeTradableEvent getTarget() {
        return nested.getTarget();
    }

    @Override
    public void done() {
        nested.done();
    }

    public PrivateOrderBook getBook(int securityId) {
        return nested.getBook(securityId);
    }

    public int longestBatch;
    public long longestBatchId;

    private long delegateTime;

    @Override
    public void pushBatch(long batchNumber, LiveEvent[] batch, int batchSize) {
        delegateTime = NanoClock.getNanoTimeNow();
        nested.pushBatch(batchNumber, batch, batchSize);
        totalPushed += batchSize;
        batches++;
        if (batchSize > longestBatch) {
            longestBatch = batchSize;
            longestBatchId = batchNumber;
        }

        if (isLive) {
            long procTime = System.nanoTime() - delegateTime;
            log.info("Pushed #" + batchNumber + ":\t" + batchSize + "\t -> " + totalPushed + "\t us " + procTime / 1_000);
            if (procTime > 1_000_000) { // 1 ms
                log.warn(batchNumber + " High process time: " + (procTime / 1_000_000) + " ms for " + batchNumber + " events");
            }
        }
    }


    public long getPushed() {
        return totalPushed;
    }

    protected int batches = 0;

    protected long totalPushed = 0;

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

    public AlgoAbstract getNested() {
        return nested;
    }

    @Override
    public void reset() {
        nested.reset();
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
