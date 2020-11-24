package algoApi;

import algoAPI.AlgoAPI;
import algoAPI.AlgoAction;
import algoAPI.AlgoOperation;
import algoAPI.AlgoResultCallback;
import algorithms.AlgoAPIImpl;
import events.LiveEvent;
import events.book.BookAtom;
import events.feed.InitializeTradableEvent;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


public abstract class AlgoAbstract implements AlgoAPI {
    private static final Logger log = Logger.getLogger(AlgoAPIImpl.class);
    InitializeTradableEvent target;
    protected final Int2ObjectHashMap<InitializeTradableEvent> initTradables = new Int2ObjectHashMap<>();
    public final static boolean busy = false;
    public final static boolean isLive = false; //GlobalProperties.getInstance().getK300FeedSource().isLive();
    // If not sim, we expect to find ourselves in the book
    public final static boolean isProd = true;

    @Override
    public void initTradable(LiveEvent event) {
        if (!(event instanceof InitializeTradableEvent)) return;
        InitializeTradableEvent initEvent = (InitializeTradableEvent) event;
        this.initTradables.put(initEvent.tradableId, initEvent);
    }

    public boolean setTarget(InitializeTradableEvent target) {
        if (!initTradables.containsKey(target.tradableId)) return false;
        this.target = target;
        log.info("Target instrument set: " + this.target);
        return true;
    }

    boolean isInitDone = false;

    @Override
    public void done() {
        isInitDone = true;
        if (target == null) {
            setTarget(initTradables.get(14028));
        }
    }

    protected int count = 0;

    @Override
    public synchronized void pushBatch(long batchNumber, LiveEvent[] liveEvents, int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            count++;
            process((BookAtom) liveEvents[i], i + 1 == batchSize);
        }
    }

    protected void process(BookAtom liveEvent, boolean isLast) {

    }

    protected AlgoResultCallback callbackListener;

    @Override
    public void addCallbackListener(AlgoResultCallback algoResultCallback) {
        callbackListener = algoResultCallback;
    }

    @Override
    public void activate(String s, AlgoAction action, AlgoOperation algoOperation) {
//        if (this.isActive() && this.getTrackers().get(target).isFreeToOperate()) {
//            this.cancelBeforeOperate(batchNumber, target, action.getOpposite(), price);
//            this.getTrackers().operate(target);
//            operateSize = Math.max(Math.min(operateSize, this.maxOrderSize), this.minOrderSize);
//        this.callbackListener.operate(ba, target, operation, true,
//                operateSize, (double) price, action, execution);
//            return action.multiplier() * operateSize;
//        } else {
//            return 0;
//        }

    }

    @Override
    public void disable(String userRef, AlgoAction algoAction, AlgoOperation algoOperation) {
        callbackListener.cancelOperate(-1L, userRef);
    }

    protected Map<String, String> settings = new HashMap<>();

    @Override
    public void setProperty(String property, String value) {
        String prevValue = this.settings.put(property, value);
        boolean change = !value.equals(prevValue);
        if (!change) return;

        log.warn("SETPROPERTY=" + property + ":" + value);
    }

}
