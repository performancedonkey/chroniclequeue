package simulation;

import algoAPI.AlgoAPI;
import consumers.BookAssembler;
import events.LiveEvent;
import events.book.BookAtom;
import org.apache.commons.lang.SystemUtils;
import org.apache.log4j.Logger;
import setaffinitymanager.SetAffinityManager;
import utils.BusyReentrantLock;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class AlgoBatcher extends DelegatorAlgoApi {
    private final Logger log = Logger.getLogger(AlgoBatcher.class);

    private final BusyReentrantLock pushBatchLock = new BusyReentrantLock();
//    private final BusyReentrantLock queueLock = new BusyReentrantLock();

    final static String threadName = "AlgoApiAsyncWorker";

    private final Worker worker;

    public AlgoBatcher(AlgoAPI nested, Logger log) {
        super(nested, log);
//        this.log = log;
        worker = new Worker();
        Thread thread = new Thread(worker, threadName);
        thread.setPriority(Thread.MAX_PRIORITY);
        thread.start();
    }

    boolean isBusy = false;

    AtomicInteger pushedEvent = new AtomicInteger();

    public void pushEvent(BookAtom event) {
        try (BusyReentrantLock.Lock ignored = pushBatchLock.lock()) {
//            queueLongs.add((long) count);
//            queueLongs.add(batchNumber);
            event.getTimestamps().setSequence(pushedEvent.incrementAndGet());
            queueEvents.add(event);
//        System.out.println("adding " + event.getTimestamps().getSequence());
        }
    }

    class Worker implements Runnable {
        volatile boolean stop = false;

        AtomicLong batchIdGenerator = new AtomicLong(0);
        //        private Long longTemp = null;
        //        private Long lastBatch = null;
        public int maxAccumulate = 0;

        public void stop() {
            this.stop = true;
        }

        public void run() {
            int cpu = -1;
            try {
                log.info("Start AlgoAPI queue worker: " + threadName);

                if (SystemUtils.IS_OS_LINUX) {
                    cpu = SetAffinityManager.getInstance().setAffinity(threadName);
                    if (cpu == -1) {
                        log.error("error setting affinity to thread " + threadName);
                    } else {
                        log.error("affinity was set ok for thread " + threadName + " to cpu " + cpu);
                    }
                }

                work();

            } catch (Exception e) {
                log.error("Exception ", e);
            } finally {
                if (SystemUtils.IS_OS_LINUX) {
                    SetAffinityManager.getInstance().releaseCpu(threadName, cpu);
                    log.error("releasing cpu " + cpu + " affinity to thread " + threadName);
                }
            }
        }

        private void work() throws InterruptedException {
            while (!stop) {
                int batchSize = sendBatch();
                if (!busy && batchSize == 0) {
                    Thread.sleep(1);
                }
            }
            log.warn("Worker thread " + Thread.currentThread().getName() + " done. " +
                    "Pushed " + getPushed() + " in " + batchIdGenerator.get() + " batches");
        }

        public boolean isBusy() {
            return isBusy;
        }

        private int sendBatch() {
            isBusy = true;
            try (BusyReentrantLock.Lock ignored = pushBatchLock.lock()) {
                Queue<LiveEvent> batch = queueEvents;
                if (batch.isEmpty()) return 0;
                int batchSize = batch.size();
                // quickly swap queues
//                try (BusyReentrantLock.Lock ignored = queueLock.lock()) {
                queueEvents = cleanQueue;

                LiveEvent[] batchArr = new LiveEvent[batchSize];
                for (int i = 0; i < batchSize; i++) {
                    batchArr[i] = batch.poll();
//                    System.out.println("polled: " + ((BookAtom) batchArr[i]).getTimestamps().getSequence());

                    // TODO What is this?
//                while (batchArr[i] == null) batchArr[i] = batch.poll();
                }
                pushBatch(batchIdGenerator.incrementAndGet(), batchArr, batchSize);

                cleanQueue = batch;

                if (batchSize > maxAccumulate) {
                    if (log.isDebugEnabled()) {
                        log.debug("New max accumulate is " + batchSize);
                    }
                    maxAccumulate = batchSize;
                }

                totalPushed += batchSize;
                isBusy = false;
            }
            return 0;
        }
    }

    // static final int maxQueue = 200;
    // was ArrayBlockingQueue - but nasty deadlock if push batch(OrderSent) blocks when queue is full
    // another way to have a lower priority queue for MC and linkedlist higher priority for everything else

    private Queue<LiveEvent> queueEvents = new ConcurrentLinkedQueue<>();
    private Queue<LiveEvent> cleanQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void setProperty(String property, String value) {
        if (property.equals("Shutdown")) {
            boolean hasMoreEvents = !queueEvents.isEmpty();
            if (hasMoreEvents) {
                while (hasMoreEvents) {
                    log.warn("Shutdown Waiting for work completion: " + queueEvents.size() + " events queued");
                    if (worker.stop)
                        worker.sendBatch();
                    hasMoreEvents = !queueEvents.isEmpty();
                }
            }
            while (isNestedBusy()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            worker.stop();

            log.error("SHUTDOWN: " + totalPushed + " pushed in " + worker.batchIdGenerator.get() + " batches / longest was " + worker.maxAccumulate);

            ((BookAssembler) getNested()).reset();
        }

        super.setProperty(property, value);
    }

    @Override
    public boolean isBusy() {
        return isNestedBusy();
    }

    private boolean isNestedBusy() {
        return worker.isBusy();
    }
}
