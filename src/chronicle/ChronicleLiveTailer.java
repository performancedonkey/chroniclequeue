package chronicle;

import algoAPI.Side;
import events.book.LeanQuote;
import events.book.Manageable;
import events.book.OrderBook;
import events.feed.InitializeTradableEvent;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import utils.LogUtil;
import utils.NanoClock;
import utils.Paths;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

public class ChronicleLiveTailer {
    private static final Logger log = Logger.getLogger(ChronicleLiveTailer.class);

    static int iter = 0;
    int tailed = 0, assimilated = 0;
    private final boolean shouldDebug = log.isDebugEnabled();
    private static String queueName = MainPlayer.fileName;

    public static void main(String[] args) {
        DOMConfigurator.configure("./log4j.xml");
        if (args.length > 0) {
            long offset = Long.parseLong(args[0]);
            NanoClock.setOffset(offset);
        }
        if (args.length > 1) {
            queueName = args[1];
        }
        String pathStr = Paths.temp_path + "chronicle/" + queueName;

        final ChronicleLiveTailer queueTailer = new ChronicleLiveTailer(pathStr);

        if (args.length > 2) {
            for (String secId : args[2].split(",")) {
                queueTailer.interesting(Integer.parseInt(secId));
            }
        }


        for (; iter < 10_000; iter++) {
            queueTailer.run();
            if (iter == 0)
                log.error("Filtering for " + queueTailer.books.keySet().stream().collect(Collectors.toList()));
        }
    }

    IntHashSet securitiesToFilter = new IntHashSet();

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
//        if (this.queue.size()==0)
//            log.error("Empty queue");
    }

    private final Int2ObjectHashMap<OrderBook> books = new Int2ObjectHashMap<>();

    Manageable current;

    public void run() {
        try {
            reset();
            this.tailer = queue.createTailer();
            this.tailer.toStart();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long start = System.currentTimeMillis();
        do {
            while ((current = readNext(tailer)) != null) {
                OrderBook book = process(current);
//                if (current.getId()==6816519491074l)
//                    System.out.println(current);
//                if (current.getType().isPrivate() && book != null && book.isValid())
//                    System.out.println(book.getTracker().calcPnL(book.getFairValue()));
            }
        } while (isSync || current != null);
        if (iter % 20 == 0)
            LogUtil.log(iter + "# Done processing batch from memory", start, assimilated);
        for (OrderBook book : books.values()) {
            if (!book.getTracker().getExecuted().isEmpty())
                LogUtil.log(book.getTracker().toString(), start);
        }
    }

    boolean isSync = false;

    private OrderBook process(Manageable current) {
        OrderBook book = assimilate(current);
        isSync |= (getAgeUs(current) < 1_000_000);
        if (isSync) consume(current, book);
        return book;
    }

    private void consume(Manageable change, OrderBook book) {
        if (tailed % 20 == 0)
            log.info(getAgeUs(current) + ": \t#" +
                    tailed + " #" + current.getTimestamps().getSequence() + " " + current);
//            consume(current, book);

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

    public static long getAgeUs(Manageable order) {
        return (NanoClock.getNanoTimeNow() - order.getTimestamp()) / 1_000;
    }

    private OrderBook assimilate(Manageable current) {
        if (!filter(current.getSecurityId()))
            return null;
        assimilated++;
        OrderBook book = getBook(current.getSecurityId());
        return book.assimilate(current);
//        if (current.getId() == 6816519491474l)
//            System.out.println(current.getLayer() + " / " + book.getSideOrders(current.getSide()).getDirtyTob() + " / " + current);

//        if (current.getType().isExecution())
//        return null;
    }

    public OrderBook getBook(int securityId) {
        if (!books.containsKey(securityId)) {
            InitializeTradableEvent ite = new InitializeTradableEvent();
            ite.tradableId = securityId;
            ite.step = 0.25f;
            ite.marketDepth = 10;
            ite.impliedLayers = 0;
            books.put(securityId, new OrderBook(ite));
        }
        return books.get(securityId);
    }

    private void reset() {
        tailed = 0;
        assimilated = 0;
        LeanQuote.resetCreated();
        for (OrderBook book : books.values()) {
            books.get(book.initEvent.tradableId).clear();
        }
    }

    final static LeanQuote.QuoteType[] quoteTypes = LeanQuote.QuoteType.values();
    final static Side[] sides = Side.values();

    public LeanQuote readNext(ExcerptTailer tailer) {
        LeanQuote next = getNext(tailer);
        if (next != null) tailed++;
        return next;
    }

    public LeanQuote getNext(ExcerptTailer tailer) {
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
        return LeanQuote.getCleanQuote();
    }

    public int getTailed() {
        return tailed;
    }
}
