//package disrupcher;
//
//import algoAPI.AlgoAPI;
//import events.LiveEvent;
//import events.book.BookAtom;
//import events.book.LeanQuote;
//import events.feed.InitializeTradableEvent;
//import org.agrona.collections.Int2ObjectHashMap;
//import trackers.OrderTracker;
//import trackers.PrivateOrderBook;
//import trackers.Tracker;
//import utils.DateUtils;
//
//import java.util.concurrent.atomic.AtomicInteger;
//
//public class BookAtomHandler extends EarlyReleaseHandler<EventHolder<BookAtom>> {
//
//    long assimilated = 0;
//
//    final AlgoAPI nested;
//    final boolean isLive;
//    final boolean isProd;
//
//    public BookAtomHandler(AlgoAPI nested, boolean isProd, boolean isLive) {
//        this.nested = nested;
//        this.isProd = isProd;
//        this.isLive = isLive;
//    }
//
//    private int batchSize = 0;
//    private LiveEvent[] batch = new LiveEvent[256];
//    public int longestBatch;
//    public int longestBatchId;
//    AtomicInteger batchId = new AtomicInteger(0);
//
//    @Override
//    public void onEvent(EventHolder<BookAtom> eventHolder, long sequence, boolean endOfBatch) {
//        batchSize++;
////        super.onEvent(eventHolder, sequence, endOfBatch);
//        onEvent(eventHolder);
////        if (batchId.get() >= 7219 && batchId.get()<=7229)
////            System.out.println(DateUtils.formatDateTimeMicro(eventHolder.getEvent().getTimestamp()) +" @ " +batchSize +
////                    " :\t " + eventHolder + " / " + eventHolder.getEvent().getLayer());
//        assimilated++;
//
//        // All private events are end of batches
//        if (((LeanQuote) eventHolder.getEvent()).isLast()) {
//            dispatch();
//        }
////        eventHolder.clear();
//    }
//
//    private void dispatch() {
//        // Dispatch batch to nested api
//        nested.pushBatch(batchId.get(), batch, batchSize);
////            int seq = (eventHolder.getEvent()).getTimestamps().getSequence();
//        if (batchSize > longestBatch) {
//            longestBatch = batchSize;
//            longestBatchId = batchId.get();
////                if (batchSize > 160)
////                    System.out.println("new longest #" + batchId + ": " + batchSize);
//        }
//        batchId.incrementAndGet();
//        batchSize = 0;
//    }
//
//    @Override
//    protected void onEvent(EventHolder<BookAtom> eventHolder) {
//        BookAtom event = eventHolder.getEvent();
//        int securityId = event.getSecurityId();
//        PrivateOrderBook book = getBook(securityId);
//        book.assimilate(event);
//
//        batch[batchSize - 1] = event;
//        if (batchSize == batch.length)
//            dispatch();
//    }
//
//    private final Int2ObjectHashMap<PrivateOrderBook> books = new Int2ObjectHashMap<>();
//
//    private void addBook(PrivateOrderBook privateBook) {
//        books.put(privateBook.initEvent.tradableId, privateBook);
//    }
//
//    public PrivateOrderBook getBook(int securityId) {
//        if (!books.containsKey(securityId)) {
//            InitializeTradableEvent ite = new InitializeTradableEvent();
//            ite.step = 0.25f;
//            ite.marketDepth = 10;
//            ite.impliedLayers = 0;
//            ite.setTradableId(securityId);
//            // isProd determines if we expect to find orders in public book
//            // live disables tracking and logging
//            addBook(new PrivateOrderBook(ite, isProd, isLive, false));
//        }
//        return books.get(securityId);
//    }
//
//    public void reset() {
//        batchId.set(0);
//        longestBatch = 0;
//        assimilated = 0;
////       f LeanQuote.resetCreated();
//        for (int securityId : books.keySet()) {
//            // Swap the private book with all the private info for a fresh one.
//            PrivateOrderBook privateBook = books.replace(securityId, new PrivateOrderBook(books.get(securityId).initEvent, isProd, isLive, false));
//            Tracker tracker = privateBook.getTracker();
//            // Log deals before we clear the data
////            logResults(privateBook, tracker);
//            tracker.detach();
//            privateBook.clear();
//        }
//    }
//
//    private void logResults(PrivateOrderBook privateBook, Tracker tracker) {
//        int nonPerf = 0;
//        int ix = 0;
//        int aggressive = 0;
//        for (OrderTracker orderTracker : privateBook.getTrackers()) {
//            ix++;
//            if (orderTracker.getPublicOrder() == null)
//                aggressive++;
//            // Only left are trades where private beat public and we were not first in queue
//            if (orderTracker.getPriority() != 0) {
//                System.out.println(ix + " " + nonPerf++ + "\t" + orderTracker.getPriority() + " / " +
//                        orderTracker.getOrdersAhead() + (orderTracker.getPublicOrder() == null ? " Agg" : " Pas ") +
//                        " P: " + orderTracker.getPublicOrder() + ":" + orderTracker + "\t" + orderTracker.ordersAheadStr)
//                ;
//            }
//        }
//        if (tracker.getVolume() > 0) {
//            System.out.println(privateBook.initEvent.tradableId + ": " + tracker);
//        }
//    }
//
//    public long getAssimilated() {
//        return assimilated;
//    }
//
//    public int getBatches() {
//        return batchId.get();
//    }
//}
