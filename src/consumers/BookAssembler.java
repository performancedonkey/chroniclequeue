package consumers;

import algoAPI.AlgoExecution;
import algoAPI.AlgoOperation;
import algoApi.AlgoAbstract;
import events.book.BookAtom;
import events.feed.InitializeTradableEvent;
import org.apache.log4j.Logger;
import trackers.OrderTracker;
import trackers.PrivateOrderBook;
import trackers.Tracker;
import utils.NanoClock;

public class BookAssembler extends AlgoAbstract {
    private static final Logger log = Logger.getLogger(BookAssembler.class);

    @Override
    protected void process(BookAtom event, boolean isLast) {
        int securityId = event.getSecurityId();
        PrivateOrderBook book = getBook(securityId);
        OrderTracker affected = book.assimilate(event);

        if (event.getType().isTicker() && event.getLayer() < -1) {
            // Fill the room in the first layer -  close the spread in the trade direction
            float price = book.getPublicBook().getSideOrders(event.getSide()).getPrice((short) -1);
            callbackListener.operate(currentBatch, price + event.getSide().getAction().toString() + generateUserRef(), AlgoOperation.quote, true,
                    1, price,
                    event.getSide().getAction(), AlgoExecution.LMT);
        }
        if (affected != null) {
            react(event, affected);
//        {
//                book.cancel(affected);
//            }
        }
    }

    private String generateUserRef() {
        InitializeTradableEvent target = getTarget();
        return target.tradableId + "_" + target.getBook().getLastAssimilated().getTimestamps().getSequence();
    }

    private boolean react(BookAtom event, OrderTracker affected) {
        if (!event.getType().isPrivate() &&
                affected.getPriority() > 1 &&
                affected.getProtection() <= 2 &&
                affected.getLayer().isTob()) {
//            log.warn("Cancel order " + affected.getPublicId() + " (" + affected.getId() + ") :" + event);
            callbackListener.cancelOperate(NanoClock.getNanoTimeNow(), affected.getCurrent().getUserRef());
            return true;
        }
        return false;
    }

    protected void logResults(PrivateOrderBook privateBook, Tracker tracker) {
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
