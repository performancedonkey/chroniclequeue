package simulation;

import events.LiveEvent;
import events.book.BookAtom;
import events.feed.InitializeTradableEvent;
import org.agrona.collections.Int2ObjectHashMap;
import trackers.OrderTracker;
import trackers.PrivateOrderBook;
import trackers.Tracker;

public class BookAssembler extends AlgoAbstract {
    @Override
    public void pushBatch(long batchNumber, LiveEvent[] batch, int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            assimilate((BookAtom) batch[i]);
        }
    }

    private void assimilate(BookAtom event) {
        int securityId = event.getSecurityId();
        PrivateOrderBook book = getBook(securityId);
        OrderTracker affected = book.assimilate(event);

        if (affected != null) {
//            react(event, affected);
        }

    }

    private void react(BookAtom event, OrderTracker affected) {
        if (!event.getType().isPrivate() &&
                affected.getPriority() <= 1 &&
                affected.getProtection() <= 2 &&
                affected.getLayer().isTob()) {
            System.out.println(event + " cancel order " + affected.getId() + " / " + affected.getPublicId());
        }
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
            // isProd determines if we expect to find orders in public book
            // live disables tracking and logging
            addBook(new PrivateOrderBook(ite, isProd, isLive, false));
        }
        return books.get(securityId);
    }

    public void reset() {
//        LeanQuote.resetCreated();
        for (int securityId : books.keySet()) {
            // Swap the private book with all the private info for a fresh one.
            PrivateOrderBook privateBook = books.replace(securityId, new PrivateOrderBook(books.get(securityId).initEvent, isProd, isLive, false));
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
                System.out.println(nonPerf++ + " " + ix + "\t" + orderTracker.getPriority() + " / " +
                        orderTracker.getOrdersAhead() + (orderTracker.getPublicOrder() == null ? " Agg" : " Pas ") +
                        " P: " + orderTracker.getPublicOrder() + ":" + orderTracker + "\t" + orderTracker.ordersAheadStr)
                ;
            }
        }
        if (tracker.getVolume() > 0) {
            System.out.println(privateBook.initEvent.tradableId + ": " + tracker);
        }
    }

}
