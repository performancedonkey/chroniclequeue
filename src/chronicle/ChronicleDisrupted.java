package chronicle;

import algoAPI.AlgoAPI;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import disrupcher.BatchHandler;
import disrupcher.EventHolder;
import events.book.BookAtom;
import events.book.LeanQuote;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.thread.NamedThreadFactory;
import org.agrona.collections.IntHashSet;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import simulation.BookAssembler;
import simulation.AlgoDelegator;
import simulation.AlgoDisruptor;
import trackers.PrivateOrderBook;
import utils.LogUtil;
import utils.NanoClock;
import utils.Paths;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

public class ChronicleDisrupted {
    private static final Logger log = Logger.getLogger(ChronicleDisrupted.class);
    public static int targetId = 14028;
    public static int iter = 0;
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

                final ChronicleDisrupted queueTailer = new ChronicleDisrupted(pathStr);

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
        if (args.length > 3) {
            targetId = Integer.parseInt(args[3]);
        }
    }

    private IntHashSet securitiesToFilter = new IntHashSet();

    public void interesting(int securityId) {
        securitiesToFilter.add(securityId);
        log.info("Filtering for " + securitiesToFilter);
    }

    public boolean verify(LeanQuote quote) {
        return verify(quote.getSecurityId());
//        return quote.getType().isPrivate();
    }

    public boolean verify(int securityId) {
        return securitiesToFilter.isEmpty() || securitiesToFilter.contains(securityId);
    }

    private Chronicle queue;
    private ExcerptTailer tailer;
    private File path;
    private AlgoDelegator<BookAtom> batcher;
    // Prod so
    BatchHandler<BookAtom> handler;

    public ChronicleDisrupted(String queueName) {
        path = new File(queueName);
        try {
            this.queue = ChronicleQueueBuilder.indexed(path).build();
        } catch (Exception e) {
            log.error("MM file not loaded: " + queueName, e);
        }

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = (int) Math.pow(2, 10);

        // Construct the Disruptor
        Disruptor<EventHolder<BookAtom>> disruptor = new Disruptor<>(EventHolder::new, bufferSize,
                new NamedThreadFactory("Disruptor"), ProducerType.SINGLE, new BusySpinWaitStrategy());

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<EventHolder<BookAtom>> ringBuffer = disruptor.getRingBuffer();
        batcher = new AlgoDisruptor<>(getAlgo(), log, ringBuffer);
        handler = new BatchHandler(batcher, 256);
        // Connect the handler
        disruptor.handleEventsWith(handler);

        // Start the Disruptor, starts all threads running
        disruptor.start();
    }

    private AlgoAPI getAlgo() {
        return new BookAssembler();
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
        tailer.nextIndex();
        current = getNext();
        while (tailer.nextIndex()) {
            LeanQuote next = getNext();
            boolean isNewBatch = isNewBatch(current, next);
            if (isNewBatch) {
                current.setLast(true);
                ++batches;
            }
            batcher.pushNext(current, isNewBatch);
//            if (isNewBatch)
//                simulateLoad(current, next);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            current = next;
        }
//        } while (isSync || current != null);
        done(start);
    }

    private LeanQuote getNext() {
        LeanQuote next;
        do {
            next = ChronicleTailer.getNext(tailer);
            tailed++;
        } while (!verify(next) && tailer.nextIndex());
        return next;
    }

    private long batchThreshold = 0l; // 50us batches
    private LeanQuote lastMover;

    private boolean isNewBatch(LeanQuote current, LeanQuote next) {
        if (current.getType().isPrivate()) return false;
        // Break Down large trades by groups of trades
        if (current.getType().isExecution() && !next.getType().isExecution()) return true;
        // Release batch if change at TOP
        if (isTobChange(current)) {
            lastMover = current;
            return true;
        }
        // Simulate batch separation
        return next.getTimestamps().getEarliestTimestamp() - current.getTimestamps().getSendingTime() > batchThreshold;
    }

    private boolean isTobChange(LeanQuote current) {
        if (!(current.getType().equals(LeanQuote.QuoteType.New) ||
                current.getType().equals(LeanQuote.QuoteType.Ticker))) return false;
        if (lastMover == null) {
            lastMover = current;
            return false;
        }

        return current.getSide().getMultiplier() * (lastMover.getPrice() - current.getPrice()) <= 0;
    }

    // 100 us
    long maxSleep = 100_000l;
    long batches = 0;

    private boolean simulateLoad(LeanQuote current, LeanQuote next) {
        if (current == null) return false;
        long timeDiff = next.getTimestamp() - current.getTimestamp();
        if (timeDiff < maxSleep) return false;
        if (timeDiff > 100_000_000) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return true;
        }
        long sleepTime = Math.min(maxSleep / 2, timeDiff);
        long start = NanoClock.getNanoTimeNow();
        // Busy wait
        while (NanoClock.getNanoTimeNow() - start < sleepTime) ;
        return true;
    }

    private void reset() {
        batches = 0;
        tailed = 0;
        handler.reset();
        batcher.reset();
        ((BookAssembler) batcher.getNested()).reset();
    }

    private void done(long start) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (iter % 2 == 0) {
            LogUtil.log(iter + "# Done processing from memory " + batcher.getLongestBatch() + " / " + batches +
                    " / " + batcher.getPushed() + " / " + handler.getBatches(), start, tailed);
            PrivateOrderBook book = ((BookAssembler) batcher.getNested()).getBook(targetId);
            if (!book.tracker.getExecuted().isEmpty())
                LogUtil.log(book.initEvent.tradableId + ": " + book.tracker.toString() +
                        " / " + book.tracker.getMin() + " / " + book.tracker.getMax(), start);
        }
    }

    public int getTailed() {
        return tailed;
    }
}
