package consumers;

import algoAPI.*;
import algoApi.AlgoAbstract;
import algoApi.AlgoBatcher;
import events.AbstractEvent;
import events.LiveEvent;
import events.book.BookAtom;
import events.book.LeanQuote;
import events.book.OrderBook;
import events.feed.InitializeTradableEvent;
import events.feed.OrderEvent;
import events.oms.*;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import simulation.SimMatchingEngine;
import trackers.OrderTracker;
import trackers.PrivateOrderBook;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class AlgoSimulator extends AlgoBatcher<BookAtom> implements AlgoResultCallback {
    protected final static Logger log = Logger.getLogger(AlgoSimulator.class);

    protected final static long processingTimeNs = 10_000;
    protected final static long networkLatency = 5_000;

    PriorityQueue<OrderEvent> incoming = new PriorityQueue<>();
    SimMatchingEngine me;

    public AlgoSimulator(AlgoAbstract nested, Logger log) {
        super(nested, log);
        addCallbackListener(this);
    }

    public SimMatchingEngine initMatchingEngine(InitializeTradableEvent initEvent) {
        PrivateOrderBook privateOrderBook = getBook(initEvent.tradableId);
        return new SimMatchingEngine(privateOrderBook);
    }

    public void pushBatch(long batchNumber, LiveEvent[] batch, int batchSize) {
        // Intercept before the incoming batch for simulated OMS responses
//        if (incoming.size() > 2) {
//            System.out.println(batchNumber + "# weird - multicancel?" + incoming);
//        }
        if (incoming.size() > 0 && me == null) {
            me = initMatchingEngine(getTarget());
        }
        OrderEvent firstInQueue = incoming.peek();

        for (int i = 0; i < batchSize; i++) {
            if (!(batch[i] instanceof BookAtom)) continue;
            BookAtom atom = (BookAtom) batch[i];
            // OrderExecutions need to be processed before new incoming orders
            boolean servicePrivateFirst = !atom.getType().isExecution() && firstInQueue != null &&
                    // New gw Request
                    firstInQueue.getTimestamp() + networkLatency < atom.getTimestamps().getGwRequestTime();
            if (servicePrivateFirst) {
                // Process the response to a private order
                ArrayList<BookAtom> outgoingEvents = me.process(firstInQueue);

                int shiftSize = outgoingEvents.size();
                // Shift batch events to make room for private and synthetic public events
                if (!makeRoom(batch, i, shiftSize, batchSize)) {
                    log.warn("No room");
                    break;
                }
                batchSize += shiftSize;
                incoming.poll();
                for (BookAtom outgoing : outgoingEvents) {
                    batch[i++] = outgoing;

                    if (outgoing.getType().equals(LeanQuote.QuoteType.OrderSentConfirm)) {
                        if (((OrderSentConfirmEvent) outgoing).getUserRef().equals(pending.getUserRef())) {
                            pending = null;
                        }
                    } else if (outgoing.getType().equals(LeanQuote.QuoteType.OrderCancelConfirm)) {
                        if (pending != null && ((OrderCancelledConfirmEvent) outgoing).getUserRef().equals(pending.getUserRef())) {
                            pending = null;
                        }
                    } else if (outgoing.getType().equals(LeanQuote.QuoteType.DealHappened)) {
                        // Aggressive matching
                        if (pending != null && ((DealHappenedEvent) outgoing).getUserRef().equals(pending.getUserRef())) {
                            pending = null;
                        }
                    }
                }
//                System.out.println(DateUtils.formatDateTimeMicro(me.getTimestamps().getSendingTime()) + " " + outgoingEvents.toString());
                outgoingEvents.clear();
                firstInQueue = incoming.peek();
            }

            // service Public
            if (me != null) {
                // Process public events for synthetic matching of working simulated orders
                if (atom.getType().isPrivate()) continue;
                ArrayList<BookAtom> outgoingEvents = me.process(atom);
                if (outgoingEvents.isEmpty()) continue;
                int shiftSize = outgoingEvents.size();
                // Shift batch events to make room for private and synthetic public events
                if (!makeRoom(batch, i, shiftSize, batchSize)) {
                    log.warn("No room");
                    break;
                }
                for (BookAtom outgoing : outgoingEvents) {
                    batch[i++] = outgoing;
                }
                batchSize += shiftSize;
//                System.out.println(DateUtils.formatDateTimeMicro(me.getTimestamps().getSendingTime()) + " " + outgoingEvents.toString());
                outgoingEvents.clear();
            }
        }
        // Pass to algorithm
        super.pushBatch(batchNumber, batch, batchSize);
//        remove from cancelPending
//        for (int i = 0; i < batchSize; i++) {
//            if (batch[i] instanceof OrderEvent){
//
//            }
//        }
    }

    private static boolean makeRoom(LiveEvent[] events, int shiftStart, int shiftSize, int batchSize) {
        if (batchSize + shiftSize > events.length) return false;
        for (int destination = batchSize + shiftSize; destination >= shiftStart + shiftSize; destination--) {
            events[destination] = events[destination - shiftSize];
        }
        return true;
    }

    private OrderEvent pending;
    private Set<String> cancelled = new HashSet<>();
    // unique across simulations
    private static final AtomicLong ordersSentCounter = new AtomicLong();

    @Override
    public void operate(long batchNumber, String userRef, AlgoOperation operation, boolean active,
                        int amount, double price, AlgoAction algoAction, AlgoExecution algoExecution) {
        OrderSentEvent orderSend = getOrderSentEvent(algoAction, (float) price, amount, userRef);

        // Network latency to be accounted for at dequeueing
        if (validate(orderSend)) {
            orderSend.setOrderId(ordersSentCounter.incrementAndGet());
            sendPrivateOrder(orderSend);
            pending = orderSend;
        } else {
//            if (orderSend.getTimestamps().getSequence()>40229824)
//            log.info(me.getTimestamps().getSendingTime() + " New rejection " + orderSend);
        }
    }

    private void sendPrivateOrder(OrderEvent outgoing) {
        if (outgoing.getTimestamps().getSequence() == 40231022)
            System.out.println(outgoing);
        pushNext(outgoing, false);
        incoming.add(outgoing);
    }

    @NotNull
    private OrderSentEvent getOrderSentEvent(AlgoAction algoAction, float price, int amount, String userRef) {
        OrderSentEvent orderSend = createOrderSend();
        InitializeTradableEvent target = getTarget();
        short layer = target.getBook().getSideOrders(algoAction.getSide()).calcLayer(price);
        orderSend.set(userRef, (Long) null, algoAction, price, amount,
                layer, AlgoOrderType.LMT, target.getBook().getTopOfBook());
        orderSend.getTimestamps().setSequence(target.getBook().getTimestamps().getSequence());
        orderSend.getTimestamps().setGwRequestTime(target.getBook().getTimestamps().getMatchingTime());
        orderSend.getTimestamps().setMatchingTime(target.getBook().getTimestamps().getSendingTime());
        orderSend.getTimestamps().setSendingTime(target.getBook().getTimestamp());
        // set arrival time to time of simulated time of order sending
        orderSend.timestamp = target.getBook().getTimestamp() + processingTimeNs;
        return orderSend;
    }

    int maxDelta = 1, softDelta = 1;

    // Guards like method to check if the algo actually should expose more
    private boolean validate(OrderEvent orderSend) {
//        if (1 == 1) return false;
        if (pending != null)
            return false;
        PrivateOrderBook book = getTargetPrivateBook();
        int outstanding = book.getOutstandingPosition();
        if (Math.abs(outstanding + orderSend.getSignedAmount()) > maxDelta)
            return false;
        // Max Delta
        int exposure = book.getOutstandingExposure(orderSend.getSide());
        // Soft Delta
        if (exposure + orderSend.getAmount() > softDelta)
            return false;
        boolean hasOrderAtPrice = false;
        for (OrderTracker working : book.getSideOrders(orderSend.getSide()).getWorking().values()) {
            hasOrderAtPrice |= working.getPrice() == orderSend.getPrice();
        }
        if (hasOrderAtPrice)
            return false;
        // Hard Delta
        return true;
    }

    private PrivateOrderBook getTargetPrivateBook() {
        return getBook(getTarget().tradableId);
    }

    private OrderSentEvent createOrderSend() {
        return new OrderSentEvent(getTarget());
    }

    @Override
    public void cancelOperate(long batchNumber, String userRef) {
        if (cancelled.contains(userRef))
            return;

        cancelled.add(userRef);
        OrderCancelledEvent orderCancel = getOrderCancelEvent(userRef);
        sendPrivateOrder(orderCancel);
//        // Network latency to be accounted for at dequeueing
////        if (validate(this.orderCancel)) {
////        orderCancel.setOrderId(orderSent.incrementAndGet());
//        pushNext(orderCancel, false);
//        incoming.add(orderCancel);
////        }
    }


    @NotNull
    private OrderCancelledEvent getOrderCancelEvent(String userRef) {
        OrderCancelledEvent orderCancel = createOrderCancel();
        orderCancel.setUserRef(userRef);
//        orderCancel.setReason(String.valueOf(batchNumber));
        OrderEvent workingOrder = getTargetPrivateBook().getWorking(userRef).getCurrent();
        orderCancel.set(workingOrder);
        OrderBook book = orderCancel.initEvent.getBook();
        orderCancel.getTimestamps().setSequence(book.getTimestamps().getSequence());
        orderCancel.getTimestamps().setGwRequestTime(book.getTimestamps().getMatchingTime());
        orderCancel.getTimestamps().setMatchingTime(book.getTimestamps().getSendingTime());
        orderCancel.getTimestamps().setSendingTime(book.getTimestamp());
        // set arrival time to time of simulated time of order sending
        orderCancel.timestamp = book.getTimestamp() + processingTimeNs;

        return orderCancel;
    }


    private OrderCancelledEvent createOrderCancel() {
        return new OrderCancelledEvent(getTarget());
    }

    @Override
    public void pushEvent(AbstractEvent abstractEvent) {
        // Bypass queue
    }

    @Override
    public void algoSetProperty(String property, String value) {
        nested.setProperty(property, value);
    }

    @Override
    public void toLog(String msg, String reason) {
        log.info(msg + " " + reason);
    }

    @Override
    public void reset() {
        super.reset();
        if (me != null)
            me.clear();
        cancelled.clear();
        pending = null;
    }
}