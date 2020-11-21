package chronicle;

import algoAPI.Side;
import events.book.LeanQuote;
import events.book.BookAtom;
import events.book.OrderBook;
import events.feed.InitializeTradableEvent;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import utils.Paths;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

public class ChronicleTailer {
    private static final Logger log = Logger.getLogger(ChronicleTailer.class);

    static String pathStr = Paths.temp_path + "chronicle/" + MainPlayer.fileName;

    int tailed = 0;
    static int iter = 1;
    private final boolean shouldDebug = log.isDebugEnabled();

    public static void main(String[] args) {
        DOMConfigurator.configure("./log4j.xml");

        for (int tailers = 0; tailers < 2; tailers++) {
            final ChronicleTailer tailer = new ChronicleTailer(pathStr);
            Executors.newSingleThreadExecutor().execute(() -> {
                for (; iter < 10_000; iter++) {
                    tailer.run();
                }
            });
        }
    }

    private Chronicle queue;
    private ExcerptTailer tailer;
    private File path;

    public ChronicleTailer(String queueName) {
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

    BookAtom current;
    boolean isLive = false;

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
                OrderBook book = assimilate(current);
//            consume(current, book);
            }
        } while (isLive);
        if (iter % 100 == 0)
            TestMap.log(iter + "# Done processing batch from memory " + tailed, start);
    }

    private void consume(BookAtom change, OrderBook book) {
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


    private OrderBook assimilate(BookAtom current) {
        int securityId = current.getSecurityId();
        if (securityId == 569388)
            return null;
        OrderBook book = getBook(securityId);
        book.initEvent.assimilate(current);
        return book;
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
        LeanQuote.resetCreated();
        for (OrderBook book : books.values()) {
            // Reset for new iteration
            books.get(book.initEvent.tradableId).clear();
        }
    }

    final static LeanQuote.QuoteType[] quoteTypes = LeanQuote.QuoteType.values();
    final static Side[] sides = Side.values();

    public LeanQuote readNext(ExcerptTailer tailer) {
        if (!tailer.nextIndex()) return null;
        LeanQuote next = getNext(tailer);
        if (next != null) tailed++;
        return next;
    }

    public static LeanQuote getNext(ExcerptTailer tailer) {
        LeanQuote quote = LeanQuote.getCleanQuote();
        tail(quote, 0, tailer);
        return quote;
    }

    public int getTailed() {
        return tailed;
    }

    public static void tail(LeanQuote quote, long seq, ExcerptTailer tailer) {
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

        quote.setLast(isLast);
        quote.setSecurityId(securityId);
        quote.set(quoteType, sides[sideId], price, amount, orderId);
        quote.set(timestamp);
        quote.getTimestamps().set(sequence, gwRequest, matchingTime, sendingTime);
        quote.setLayer(OrderBook.LAYER_NOT_SET);
    }


}
