package chronicle;

import algoAPI.AlgoAPI;
import events.book.BookAtom;
import events.book.LeanQuote;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import org.agrona.collections.IntHashSet;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import simulation.AlgoBatcher;
import simulation.BookAssembler;
import simulation.AlgoDelegator;
import trackers.PrivateOrderBook;
import utils.LogUtil;
import utils.NanoClock;
import utils.Paths;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

public class ChronicleBatcher {
    private static final Logger log = Logger.getLogger(ChronicleBatcher.class);
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

                final ChronicleBatcher queueTailer = new ChronicleBatcher(pathStr);

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

    public boolean validate(int securityId) {
        return securitiesToFilter.isEmpty() || securitiesToFilter.contains(securityId);
    }

    public boolean validate(LeanQuote quote) {
        return validate(quote.getSecurityId());
//        return quote.getType().isPrivate();
    }

    private Chronicle queue;
    private ExcerptTailer tailer;
    private File path;
    private AlgoDelegator<BookAtom> batcher;

    public ChronicleBatcher(String queueName) {
        path = new File(queueName);
        try {
            this.queue = ChronicleQueueBuilder.indexed(path).build();
        } catch (Exception e) {
            log.error("MM file not loaded: " + queueName, e);
        }

//        // Specify the size of the ring buffer, must be power of 2.
//        int bufferSize = (int) Math.pow(2, 10);
//
//        // Construct the Disruptor
//        Disruptor<EventHolder<BookAtom>> disruptor = new Disruptor<>(EventHolder::new, bufferSize,
//                new NamedThreadFactory("Disruptor"), ProducerType.SINGLE, new BusySpinWaitStrategy());
//
//        // Get the ring buffer from the Disruptor to be used for publishing.
//        RingBuffer<EventHolder<BookAtom>> ringBuffer = disruptor.getRingBuffer();
        batcher = new AlgoBatcher<>(getAlgo(), log);
//        handler = new BatchHandler(batcher);
//        // Connect the handler
//        disruptor.handleEventsWith(handler);
//
//        // Start the Disruptor, starts all threads running
//        disruptor.start();
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
            }
            batcher.pushNext(current, isNewBatch);
//            if (isNewBatch)
//                simulateLoad(current, next);
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
        } while (!validate(next) && tailer.nextIndex());
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
        tailed = 0;
//        handler.reset();
        batcher.reset();
        ((BookAssembler) batcher.getNested()).reset();
    }

    private void done(long start) {
        if (iter % 10 == 0) {
            LogUtil.log(iter + "# Done processing from memory " + batcher.getLongestBatch() +
                    " / " + batcher.getBatches() + " / " + batcher.getPushed(), start, tailed);
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
