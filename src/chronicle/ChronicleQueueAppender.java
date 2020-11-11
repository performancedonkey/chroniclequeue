package chronicle;

import events.book.Manageable;
import events.utils.ExchangeTimestampP;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import org.agrona.collections.IntHashSet;
import org.apache.log4j.Logger;
import utils.NanoClock;
import utils.Paths;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class ChronicleQueueAppender {
    private static Logger log = Logger.getLogger(ChronicleQueueAppender.class);

    public static String path_cq = Paths.temp_path + "chronicle/";
    private Chronicle queue;
    private ExcerptAppender appender;

    //    private static final long HOW_MANY = 1_000_000L;
//    private ExcerptTailer tailer;
//    private final LongValue key = Values.newHeapInstance(LongValue.class);
    String queueName;
    File path;
    IntHashSet securitiesToFilter = new IntHashSet();

    public ChronicleQueueAppender(String queueName) {
        this(path_cq, queueName);
    }

    public ChronicleQueueAppender(String pathStr, String queueName) {
        path = new File(pathStr);
        path.mkdirs();
//            path = Files.createTempDirectory(path + "/" + "chronicle").toFile();
        path = new File(pathStr + queueName);
        this.queueName = queueName;
        //        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(path.getAbsolutePath().replace("\\","/")+"/"+"metadata.cq4t").build()) {
        try {
            this.queue = ChronicleQueueBuilder.indexed(path).build();
            this.appender = queue.createAppender();
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("Appending to " + path);
    }

    public void interesting(int securityId) {
        securitiesToFilter.add(securityId);
        log.info("Filtering for " + securitiesToFilter);
    }

    public boolean filter(int securityId) {
        return securitiesToFilter.isEmpty() || securitiesToFilter.contains(securityId);
    }

    public boolean append(Manageable quote) {
        // SW filtering
        if (quote == null || quote.getType() == null ||
                !filter(quote.getSecurityId()))
            return false;

        ExchangeTimestampP exchangeTimestamp = quote.getTimestamps();

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
        for (Long aLong : Arrays.asList(quote.getTimestamp(),
                exchangeTimestamp.getSendingTime(),
                exchangeTimestamp.getMatchingTime(),
                exchangeTimestamp.getGwRequestTime()
        )) {
            appender.writeLong(aLong);
        }

        appender.close();
        appended++;
        return true;
    }

    private int appended;

    public long getSize() {
        return appended;
    }

    public ExcerptTailer createTailer() throws IOException {
        return queue.createTailer();
    }

    public int getAppended() {
        return appended;
    }
}
