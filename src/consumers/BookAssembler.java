package consumers;

import events.LiveEvent;
import events.book.BookAtom;
import events.book.LeanQuote;
import events.feed.InitializeTradableEvent;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.log4j.Logger;
import simulation.AlgoAbstract;
import trackers.OrderTracker;
import trackers.PrivateOrderBook;
import trackers.Tracker;

public class BookAssembler extends AlgoAbstract {
    private final Logger log = Logger.getLogger(BookAssembler.class);

    int count = 0;

    @Override
    public synchronized void pushBatch(long batchNumber, LiveEvent[] liveEvents, int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            pushEvent(batchNumber, (BookAtom) liveEvents[i]);
        }
    }

//    int lastPushed = 0;

    private synchronized void pushEvent(long batchNumber, BookAtom liveEvent) {
        count++;
        LeanQuote event = (LeanQuote) liveEvent;
        int securityId = event.getSecurityId();
        PrivateOrderBook book = getBook(securityId);

        book.initEvent.assimilate(event);
//        log.info(batchNumber + "\t" + count + "\t" + book.getTimestamps().getSequence() + "\t" + event.getLayer() + "\t" + event);
//        int cur = liveEvent.getTimestamps().getSequence();
//        if (cur > lastPushed + 1)
//            System.out.println(cur);
//        lastPushed = cur;
    }

    private final Int2ObjectHashMap<PrivateOrderBook> books = new Int2ObjectHashMap<>();

    public void addBook(PrivateOrderBook privateBook) {
        books.put(privateBook.initEvent.tradableId, privateBook);
    }

    public PrivateOrderBook getBook(int securityId) {
        if (!books.containsKey(securityId)) {
            InitializeTradableEvent ite = new InitializeTradableEvent();
            ite.step = 0.25f;
            ite.marketDepth = 10;
            ite.impliedLayers = 0;
            ite.setTradableId(securityId);
            addBook(new PrivateOrderBook(ite));
        }
        return books.get(securityId);
    }

    public void reset() {
        LeanQuote.resetCreated();
        for (int securityId : books.keySet()) {
            // Swap the private book with all the private info for a fresh one.
            PrivateOrderBook privateBook = books.replace(securityId, new PrivateOrderBook(books.get(securityId).initEvent));
            // Log deals before we clear the data
            Tracker tracker = privateBook.getTracker();
            int nonPerf = 0;
            int ix = 0;
            int aggressive = 0;
            for (OrderTracker orderTracker : privateBook.getTrackers()) {
                ix++;
                if (orderTracker.getPublicOrder() == null)
                    aggressive++;
                // Only left are trades where private beat public and we were not first in queue
                if (orderTracker.getPriority() != 0) {
//                    System.out.println(ix + " " + nonPerf++ + "\t" + orderTracker.getPriority() + " / " +
//                            orderTracker.getOrdersAhead() + (orderTracker.getPublicOrder() == null ? " Agg" : " Pas ") +
//                            " P: " + orderTracker.getPublicOrder() + ":" + orderTracker + "\t" + orderTracker.ordersAheadStr)
                    ;
                }
            }
            if (tracker.getVolume() != 0) {
//privateBook.serialize();
//                System.out.println(privateBook.initEvent.tradableId + ": " + tracker);
            }
            tracker.detach();
            privateBook.clear();
        }
    }

}
