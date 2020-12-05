package algoApi;

import algoAPI.AlgoAPI;
import algoAPI.AlgoAction;
import algoAPI.AlgoOperation;
import algoAPI.AlgoResultCallback;
import algorithms.AlgoAPIImpl;
import chronicle.ChronicleBatcher;
import events.LiveEvent;
import events.book.BookAtom;
import events.feed.InitializeTradableEvent;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.log4j.Logger;
import trackers.PrivateOrderBook;
import trackers.Tracker;

import java.util.HashMap;
import java.util.Map;


public abstract class AlgoAbstract implements AlgoAPI {
    private static final Logger log = Logger.getLogger(AlgoAPIImpl.class);
    protected final Int2ObjectHashMap<InitializeTradableEvent> initTradables = new Int2ObjectHashMap<>();
    protected final Int2ObjectHashMap<PrivateOrderBook> books = new Int2ObjectHashMap<>();
    private InitializeTradableEvent target;

    public final static boolean busy = false;
    public final static boolean isLive = false; //GlobalProperties.getInstance().getK300FeedSource().isLive();
    // If not sim, we expect to find ourselves in the book
    public final static boolean isProd = true;
    public final static boolean toLog = true;

    @Override
    public void initTradable(LiveEvent event) {
        if (!(event instanceof InitializeTradableEvent)) return;
        InitializeTradableEvent initEvent = (InitializeTradableEvent) event;
        this.initTradables.put(initEvent.tradableId, initEvent);
        this.books.put(initEvent.tradableId, new PrivateOrderBook(initEvent, isProd, true, toLog));

        if (initEvent.tradableId == ChronicleBatcher.targetId) {
            setTarget(initEvent);
        }
    }

    public boolean setTarget(InitializeTradableEvent target) {
        if (!initTradables.containsKey(target.tradableId)) return false;
        this.target = target;
        log.info("Target instrument set: " + this.target);
        return true;
    }

    public InitializeTradableEvent getTarget() {
        return target;
    }

    boolean isInitDone = false;

    @Override
    public void done() {
        isInitDone = true;
    }

    protected int count = 0;
    protected long currentBatch;

    @Override
    public synchronized void pushBatch(long batchNumber, LiveEvent[] batch, int batchSize) {
        currentBatch = batchNumber;
        for (int i = 0; i < batch.length; i++) {
            if (i < batchSize) {
                count++;
                process((BookAtom) batch[i], i + 1 == batchSize);
            } else { // Clear batch. Only while testing!
                if (batch[i] == null) break;
                batch[i] = null;
            }
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

    public PrivateOrderBook getBook(int securityId) {
        if (!books.containsKey(securityId)) {
            InitializeTradableEvent ite = initTradables.get(securityId);
            if (ite == null) {
                ite = new InitializeTradableEvent();
                ite.step = 0.25f;
                ite.marketDepth = 10;
                ite.impliedLayers = 0;
                ite.setTradableId(securityId);
                // isProd determines if we expect to find orders in public book
                // live disables tracking and logging
            }
            initTradable(ite);
        }
        return books.get(securityId);
    }

    public void reset() {
//        LeanQuote.resetCreated();
        for (int securityId : books.keySet()) {
            // Swap the private book with all the private info for a fresh one.
//            PrivateOrderBook privateBook = books.replace(securityId, new PrivateOrderBook(books.get(securityId).initEvent, isProd, isLive, false));
            PrivateOrderBook privateBook = books.get(securityId);
            Tracker tracker = privateBook.getTracker();
            // Log deals before we clear the data
//            logResults(privateBook, tracker);
            tracker.detach();
            privateBook.clear();
        }
        books.clear();
    }
}
