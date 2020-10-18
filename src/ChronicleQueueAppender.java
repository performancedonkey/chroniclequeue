import algoAPI.Side;
import events.book.LeanQuote;
import events.book.Manageable;
import events.utils.ExchangeTimestamp;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class ChronicleQueueAppender {

    private Chronicle queue;
    private ExcerptAppender appender;
    File path = new File("C:/Users/Mati/AppData/Local/Temp");

//    private static final long HOW_MANY = 1_000_000L;
//    private ExcerptTailer tailer;
//    private final LongValue key = Values.newHeapInstance(LongValue.class);

    public ChronicleQueueAppender(String queueName) {
        path = new File(path + "/" + "DB/chronicle");
        path.mkdirs();
//            path = Files.createTempDirectory(path + "/" + "chronicle-queue").toFile();
        path = new File(path + "/" + queueName);

        //        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(path.getAbsolutePath().replace("\\","/")+"/"+"metadata.cq4t").build()) {
        try {
            this.queue = ChronicleQueueBuilder.indexed(path).build();
            this.appender = queue.createAppender();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void append(Manageable quote) {
        ExchangeTimestamp exchangeTimestamp = quote.getTimestamps();

        // call method on the interface to send messages
        appender.startExcerpt();

        appender.writeInt(quote.getSecurityId());
        appender.writeInt(exchangeTimestamp.getSequence());

        appender.writeByte(quote.getType().ordinal());
        appender.writeByte(quote.getSide().ordinal());
        appender.writeFloat(quote.getPrice());
        appender.writeInt(quote.getAmount());
        appender.writeLong(quote.getId());
        // Timestamps
        for (Long aLong : Arrays.asList(exchangeTimestamp.getGwRequestTime(), exchangeTimestamp.getMatchingTime(),
                exchangeTimestamp.getSendingTime(), quote.getTimestamp())) {
            appender.writeLong(aLong);
        }

        appender.close();
        appended++;
    }

    private int appended;

    public long getSize() {
        return appended;
    }

    public ExcerptTailer createTailer() throws IOException {
        return queue.createTailer();
    }
}
