import algoAPI.Side;
import events.book.LeanQuote;
import events.book.Manageable;
import events.book.OrderBook;
import events.feed.InitializeTradableEvent;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.log4j.Logger;
import utils.DateUtils;

import java.io.File;
import java.io.IOException;

public class ChronicleTailer {
    private static Logger log = Logger.getLogger(ChronicleTailer.class);

    static String queuePath = "C:/Users/Mati/AppData/Local/Temp/DB/chronicle/";
    static String readFilename = MainPlayer.fileName;

    static String pathStr = queuePath + readFilename;

    static int tailed = 0;
    static int iter = 1;

    public static void main(String[] args) {
        ChronicleTailer tailer = new ChronicleTailer(pathStr);
        for (; iter < 10_000; iter++)
            tailer.run();
    }

    Int2ObjectHashMap<OrderBook> books = new Int2ObjectHashMap<>();

    private void run() {
        try {
            initBooks();
            this.tailer = queue.createTailer();
            this.tailer.toStart();
        } catch (IOException e) {
            e.printStackTrace();
        }

        tailed = 0;
        long start = System.currentTimeMillis();
        Manageable event;
        while ((event = getNext(tailer)) != null) {
            try {
                int securityId = event.getSecurityId();
                if (securityId == 569388)
                    continue;
                tailed++;

                OrderBook book = getBook(securityId);
                book.assimilate(event);
                // Necessary for spread calculation
                book.flush();
//                    System.out.println(DateUtils.formatDateTimeMicro(event.getTimestamp()));

//                if (tailed % (100_000 + iter) == 0) {
//                    System.out.println(tailed + "# " + book.initEvent.tradableId + ": " + book.bookVolume +
//                            " | " + book.getTopOfBook() +
//                            " | " + book.getSideOrders(Side.bid).getSize() + " | " + book.getSideOrders(Side.ask).getSize());
//                }

                if (log.isDebugEnabled())
                    System.out.println(tailed + "# " + securityId + ": " +
                            book.getSideOrders(Side.bid).getDirtyTob() + " | " + book.getSideOrders(Side.ask).getDirtyTob()
                            + " \t | " + event);
            } catch (Exception e) {
                log.error("WTF" + event, e);
            }
        }

        TestMap.log(iter + "# Done processing batch from memory " + tailed, start);
    }

    private OrderBook getBook(int securityId) {
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

    private void initBooks() {
        for (OrderBook book : books.values()) {
            books.replace(book.initEvent.tradableId, new OrderBook(book.initEvent));
            // SMTH not fully cooked with the clearing mechanism. Top of book
//            book.clear();
        }
    }

    private Chronicle queue;
    private ExcerptTailer tailer;
    private File path;

    public ChronicleTailer(String queueName) {
        path = new File("DB/chronicle");
        path.mkdirs();
        path = new File(queueName);
        try {
            this.queue = ChronicleQueueBuilder.indexed(path).build();
        } catch (Exception e) {
            System.out.println("Ahhh");
        }
    }

    public static LeanQuote getNext(ExcerptTailer tailer) {
        if (!tailer.nextIndex())
            return null;

        int securityId = tailer.readInt();
        int sequence = tailer.readInt();
        byte typeId = tailer.readByte();
        LeanQuote.QuoteType quoteType = LeanQuote.QuoteType.values()[typeId];
        byte sideId = tailer.readByte();
        Side side = Side.values()[sideId];
        // Original code was float 64 double
        float price = tailer.readFloat();
        int amount = tailer.readInt();
        long orderId = tailer.readLong();
        long gwRequest = tailer.readLong();
        long matchingTime = tailer.readLong();
        long sendingTime = tailer.readLong();
        long timestamp = tailer.readLong();

        // do something with values.
        LeanQuote quote = new LeanQuote();
        quote.set(quoteType, side, price, amount, orderId);
        quote.set(timestamp);
        quote.getTimestamps().set(sequence, gwRequest, matchingTime, sendingTime);

        quote.setSecurityId(securityId);
        return quote;
    }
}
