package chronicle;

import algoAPI.Side;
import events.book.BookAtom;
import events.book.LeanQuote;
import events.book.OrderBook;
import events.feed.InitializeTradableEvent;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.jetbrains.annotations.NotNull;
import trackers.OrderTracker;
import trackers.PrivateOrderBook;
import trackers.Tracker;
import utils.LogUtil;
import utils.NanoClock;
import utils.Paths;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

public class ChronicleLiveTailer {
    private static final Logger log = Logger.getLogger(ChronicleLiveTailer.class);

    static int iter = 0;
    int tailed = 0, assimilated = 0;
    private final boolean shouldDebug = log.isDebugEnabled();
    private static String queueName = MainPlayer.fileName;

    public static void main(String[] args) {
        DOMConfigurator.configure("./log4j.xml");

        if (args.length > 0) {
            queueName = args[0];
        }
        String pathStr = Paths.temp_path + "chronicle/" + queueName;

        for (int tailers = 0; tailers < 1; tailers++) {
            Executors.newSingleThreadExecutor().execute(() -> {

                final ChronicleLiveTailer queueTailer = new ChronicleLiveTailer(pathStr);

                if (args.length > 1) {
                    for (String secId : args[1].split(",")) {
                        queueTailer.interesting(Integer.parseInt(secId));
                    }
                }

                for (; iter < 10_000; iter++) {
                    queueTailer.run();
                    if (iter == 0)
                        log.error("Filtering for " + queueTailer.securitiesToFilter.toString());
                }
            });
        }

        if (args.length > 2) {
            long offset = Long.parseLong(args[2]);
            NanoClock.setOffset(offset);
        }
    }

    private IntHashSet securitiesToFilter = new IntHashSet();

    public void interesting(int securityId) {
        securitiesToFilter.add(securityId);
        log.info("Filtering for " + securitiesToFilter);
    }

    public boolean filter(int securityId) {
        return securitiesToFilter.isEmpty() || securitiesToFilter.contains(securityId);
    }

    private Chronicle queue;
    private ExcerptTailer tailer;
    private File path;

    public ChronicleLiveTailer(String queueName) {
        path = new File(queueName);
        try {
            this.queue = ChronicleQueueBuilder.indexed(path).build();
        } catch (Exception e) {
            log.error("MM file not loaded: " + queueName, e);
        }
    }

    private final Int2ObjectHashMap<PrivateOrderBook> books = new Int2ObjectHashMap<>();

    BookAtom current;

    public void run() {
        try {
            reset();
            this.tailer = queue.createTailer();
            this.tailer.toStart();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long start = System.nanoTime();
        do {
            while ((current = readNext(tailer)) != null) {
//                if (current.getTimestamps().getMatchingTime() >= 1599724474768514600l-1_000_000)
//                    System.out.println(current.getTimestamps().getMatchingTime()+"\t"+ current);
//                if (current.getId() == 6816519480885l) {
//                    System.out.println(tailed + " " + current.getTimestamps().getSendingTime() + "\t" + current);
//                }
//                System.out.println(current.getTimestamps().getSequence());
                OrderBook book = process(current);
            }
        } while (isSync || current != null);
        done(start);
    }

    private void done(long start) {
        if (iter % 20 == 0) {
            LogUtil.log(iter + "# Done processing from memory", start, assimilated);
            for (PrivateOrderBook book : books.values()) {
                if (!book.tracker.getExecuted().isEmpty())
//                    LogUtil.log(book.initEvent.tradableId + ": " + book.tracker.toString(), start);
                    LogUtil.log(book.initEvent.tradableId + ": " + book.tracker.toString() +
                            " / " + book.tracker.getMin() + " / " + book.tracker.getMax(), start);

            }
        }
    }

    boolean isSync = false;

    private OrderBook process(BookAtom current) {
        OrderBook book = assimilate(current);
        isSync |= (getAgeUs(current) < 1_000_000);
        if (isSync) consume(current, book);
        return book;
    }

    private void consume(BookAtom change, OrderBook book) {
        if (tailed % 20 == 0)
            log.info(getAgeUs(current) + ": \t#" +
                    tailed + " #" + current.getTimestamps().getSequence() + " " + current);

//                if (current.getType().isPrivate() && book != null && book.isValid())
//                    System.out.println(book.getTracker().calcPnL(book.getFairValue()));

        // Leave as part of consuming logic alone!
//                book.flush();
//                    System.out.println(DateUtils.formatDateTimeMicro(event.getTimestamp()));

//                if (tailed % (100_000 + iter) == 0) {
//                    System.out.println(tailed + "# " + book.initEvent.tradableId + ": " + book.bookVolume +
//                            " | " + book.getTopOfBook() +
//                            " | " + book.getSideOrders(Side.bid).getSize() + " | " + book.getSideOrders(Side.ask).getSize());
//                }

        if (shouldDebug)
            log.info(tailed + "# " + change.getSecurityId() + ": " +
                    book.getSideOrders(Side.bid).getDirtyTob() + " | " +
                    book.getSideOrders(Side.ask).getDirtyTob()
                    + " \t | " + this.current);
    }

    public static long getAgeUs(BookAtom order) {
        return (NanoClock.getNanoTimeNow() - order.getTimestamp()) / 1_000;
    }

    private OrderBook assimilate(BookAtom current) {
        if (!filter(current.getSecurityId()))
            return null;
        assimilated++;
        PrivateOrderBook privateBook = getBook(current.getSecurityId());
        privateBook.assimilate(current);
//        System.out.println(current.getLayer() + " / " + privateBook.getPublicBook().getSideOrders(current.getSide()).getDirtyTob() + " / " + current);
        return privateBook.getPublicBook();
    }

    public PrivateOrderBook getBook(int securityId) {
        if (!books.containsKey(securityId)) {
            InitializeTradableEvent ite = new InitializeTradableEvent();
            // TODO set dynamically per instrument
            ite.step = 0.25f;
            ite.marketDepth = 10;
            ite.impliedLayers = 0;
            ite.setTradableId(securityId);
            books.put(securityId, new PrivateOrderBook(ite));
        }
        return books.get(securityId);
    }

    private void reset() {
        isSync = false;
        tailed = 0;
        assimilated = 0;
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

    final static LeanQuote.QuoteType[] quoteTypes = LeanQuote.QuoteType.values();
    final static Side[] sides = Side.values();

    public LeanQuote readNext(ExcerptTailer tailer) {
        LeanQuote next = getNext(tailer);
        if (next != null) tailed++;
        return next;
    }

    public LeanQuote getNext(@NotNull ExcerptTailer tailer) {
        if (!tailer.nextIndex())
            return null;

        boolean isLast = true;
        byte isOms = 1;
        int securityId = tailer.readInt();
        int sequence = tailer.readInt();
        byte typeId = tailer.readByte();
        LeanQuote.QuoteType quoteType = quoteTypes[typeId];
        byte sideId = tailer.readByte();
        // Original code was float 64 double
        float price = tailer.readFloat();
        // TODO - once and for all. Always can downcast to float
//        long encodedPrice = tailer.readDouble();
//        double price = tailer.readDouble();
        int amount = tailer.readInt();
        long orderId = tailer.readLong();
        long timestamp = tailer.readLong();
        // 1 MTU = 40 Bytes
        // Extra Data
        long sendingTime = tailer.readLong();
        long matchingTime = tailer.readLong();
        long gwRequest = tailer.readLong();

        LeanQuote quote = getQuote(false);
        quote.setLast(isLast);
        quote.setSecurityId(securityId);
        quote.set(quoteType, sides[sideId], price, amount, orderId);
        quote.set(timestamp);
        quote.getTimestamps().set(sequence, gwRequest, matchingTime, sendingTime);
        quote.setLayer(OrderBook.LAYER_NOT_SET);
        return quote;
    }

    LeanQuote reusableQuote = new LeanQuote();

    private LeanQuote getQuote(boolean reuse) {
        if (reuse) return reusableQuote;
        return new LeanQuote(); //LeanQuote.getCleanQuote();
    }

    public int getTailed() {
        return tailed;
    }
}
