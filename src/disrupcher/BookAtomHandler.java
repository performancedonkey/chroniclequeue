package disrupcher;

import events.book.BookAtom;
import events.book.LeanQuote;
import events.feed.InitializeTradableEvent;
import org.agrona.collections.Int2ObjectHashMap;
import trackers.OrderTracker;
import trackers.PrivateOrderBook;
import trackers.Tracker;

public class BookAtomHandler extends EarlyReleaseHandler<EventHolder<BookAtom>> {

    long assimilated = 0;

    boolean isLive = false;
    boolean isProd = false;

    public BookAtomHandler(boolean isLive, boolean isProd) {
        this.isLive = isLive;
        this.isProd = isProd;
    }

    @Override
    public void onEvent(EventHolder<BookAtom> eventHolder, long sequence, boolean endOfBatch) {
        processEvent(eventHolder);
        assimilated++;

//        eventHolder.clear();
//        if (endOfBatch) {
//            System.out.println(sequence);
//        }
    }

    @Override
    protected void processEvent(EventHolder<BookAtom> eventHolder) {
        BookAtom event = eventHolder.getEvent();
        int securityId = event.getSecurityId();
        PrivateOrderBook book = getBook(securityId);
        book.assimilate(event);
    }

    private final Int2ObjectHashMap<PrivateOrderBook> books = new Int2ObjectHashMap<>();

    private void addBook(PrivateOrderBook privateBook) {
        books.put(privateBook.initEvent.tradableId, privateBook);
    }

    public PrivateOrderBook getBook(int securityId) {
        if (!books.containsKey(securityId)) {
            InitializeTradableEvent ite = new InitializeTradableEvent();
            ite.step = 0.25f;
            ite.marketDepth = 10;
            ite.impliedLayers = 0;
            ite.setTradableId(securityId);
            // TODO pass as argument if we expect to find orders in public book
            addBook(new PrivateOrderBook(ite)); // , isProd));
        }
        return books.get(securityId);
    }

    public void reset() {
        assimilated = 0;
        LeanQuote.resetCreated();
        for (int securityId : books.keySet()) {
            // Swap the private book with all the private info for a fresh one.
            PrivateOrderBook privateBook = books.replace(securityId, new PrivateOrderBook(books.get(securityId).initEvent));
            Tracker tracker = privateBook.getTracker();
            // Log deals before we clear the data
//            logResults(privateBook, tracker);
            tracker.detach();
            privateBook.clear();
        }
    }

    private void logResults(PrivateOrderBook privateBook, Tracker tracker) {
        int nonPerf = 0;
        int ix = 0;
        int aggressive = 0;
        for (OrderTracker orderTracker : privateBook.getTrackers()) {
            ix++;
            if (orderTracker.getPublicOrder() == null)
                aggressive++;
            // Only left are trades where private beat public and we were not first in queue
            if (orderTracker.getPriority() != 0) {
                System.out.println(ix + " " + nonPerf++ + "\t" + orderTracker.getPriority() + " / " +
                        orderTracker.getOrdersAhead() + (orderTracker.getPublicOrder() == null ? " Agg" : " Pas ") +
                        " P: " + orderTracker.getPublicOrder() + ":" + orderTracker + "\t" + orderTracker.ordersAheadStr)
                ;
            }
        }
        if (tracker.getVolume() > 0) {
            System.out.println(privateBook.initEvent.tradableId + ": " + tracker);
        }
    }

    public long getAssimilated() {
        return assimilated;
    }
}
