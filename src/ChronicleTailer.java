import algoAPI.Side;
import events.book.LeanQuote;
import events.book.Manageable;
import events.book.OrderBook;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import org.agrona.collections.Int2ObjectHashMap;

import java.io.File;

public class ChronicleTailer {

    static String readFilename = "RTAlgorithms_200910_020726_1603046265919";
    static String pathStr = "C:/Users/Mati/AppData/Local/Temp/DB/chronicle/" + readFilename;
    static int tailed = 0;
    static int iter = 1;

    public static void main(String[] args) {
        for (; iter < 10_000; iter++)
            run();
    }

    private static void run() {
        Int2ObjectHashMap<OrderBook> books = new Int2ObjectHashMap<>();
        ChronicleTailer tailer = new ChronicleTailer(pathStr);
        Object event = null;
        tailed = 0;
        long start = System.currentTimeMillis();
        do {
            try {
                event = getNext(tailer.tailer);
                tailed++;
                if (!(event instanceof Manageable)) continue;
                int securityId = ((Manageable) event).getSecurityId();
                if (!books.containsKey(securityId))
                    books.put(securityId, new OrderBook(10, 0.5f, false));
                OrderBook book = books.get(securityId);
                book.assimilate((Manageable) event);
                book.flush();
                if (tailed % (100_000 + iter) == 0)
                    System.out.println(tailed + "# " + securityId + ": " + book.bookVolume + " | " + book.getTopOfBook() +
                            " | " + book.getSideOrders(Side.bid).getSize() + " | " + book.getSideOrders(Side.ask).getSize());
            } catch (Exception e) {
                System.out.println(e);
            }

        } while (event != null);

        TestMap.log(iter + "# Done loading from memory " + tailed, start);
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
            this.tailer = queue.createTailer();
            this.tailer.toStart();
        } catch (Exception e) {
            System.out.println("Ahhh");
        }
    }

    public static Object getNext(ExcerptTailer tailer) {
        if (!tailer.nextIndex()) return null;
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
