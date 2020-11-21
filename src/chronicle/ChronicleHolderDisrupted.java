package chronicle;

import algoAPI.AlgoAPINull;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import disrupcher.EventHolder;
import disrupcher.BookAtomHandler;
import events.book.BookAtom;
import events.book.LeanQuote;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import org.agrona.collections.IntHashSet;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import simulation.DisruptedAlgoApi;
import utils.NanoClock;
import utils.Paths;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

public class ChronicleHolderDisrupted {
    private static final Logger log = Logger.getLogger(ChronicleHolderDisrupted.class);

    static int iter = 0;
    int tailed = 0;
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

                final ChronicleHolderDisrupted queueTailer = new ChronicleHolderDisrupted(pathStr);

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
    private DisruptedAlgoApi<BookAtom> batcher;
    BookAtomHandler handler = new BookAtomHandler(false, true);

    public ChronicleHolderDisrupted(String queueName) {
        path = new File(queueName);
        try {
            this.queue = ChronicleQueueBuilder.indexed(path).build();
        } catch (Exception e) {
            log.error("MM file not loaded: " + queueName, e);
        }

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = (int) Math.pow(2, 10);

        // Construct the Disruptor
        Disruptor<EventHolder<BookAtom>> disruptor = new Disruptor<>(EventHolder::new, bufferSize, DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE, new SleepingWaitStrategy()
        );
        // Connect the handler
        disruptor.handleEventsWith(handler);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<EventHolder<BookAtom>> ringBuffer = disruptor.getRingBuffer();
        batcher = new DisruptedAlgoApi<>(new AlgoAPINull(), log, ringBuffer);
    }

    LeanQuote current = null;

    public void run() {
        try {
            reset();
            this.tailer = queue.createTailer();
            this.tailer.toStart();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long start = System.nanoTime();
//        do {
        while (tailer.nextIndex()) {
            LeanQuote next = ChronicleTailer.getNext(tailer);
            simulateLoad(current, next);
            batcher.pushNext(next);
            current = next;
            tailed++;
        }
//        } while (isSync || current != null);
        done(start);
    }

    private void simulateLoad(LeanQuote current, LeanQuote next) {
        if (current == null) return;
        long timeDiff = next.getTimestamp() - current.getTimestamp();
        if (timeDiff < 500_000_000) return;
        long sleepTime = Math.max(1, timeDiff / 100_000_000);
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void reset() {
        tailed = 0;
        handler.reset();
    }

    private void done(long start) {
    }

    public int getTailed() {
        return tailed;
    }
}
