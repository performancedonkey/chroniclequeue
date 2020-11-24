package chronicle;

import binary.AbstractFileReader;
import events.AbstractEvent;
import events.LiveEvent;
import events.book.BookAtom;
import events.book.LeanQuote;
import events.feed.InitializeTradableEvent;
import events.feed.MarketOrder;
import events.feed.OrderEvent;
import events.rt.HitAndShadowEvent;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import recorder.RecordingPlayer;
import utils.LogUtil;

import java.io.File;

public class MainPlayer {
    private static Logger log = Logger.getLogger(MainPlayer.class);

    public static String recs_path = "C:/Users/Mati/Documents/Player/liveRecordings/";
    public static String fileName = "RTAlgorithms_200910_020726";
    public static String target = "F MINISP SEP20";
    //    args = RTAlgorithms_200910_020726 "F MINISP SEP20"
    //    args = RTAlgorithms_200902_094745 "F DAX SEP20"

    static InitializeTradableEvent initEvent;

    static ChronicleQueueAppender queueAppender;

    public static void main(String[] args) {
        DOMConfigurator.configure("./log4j.xml");
        if (args.length > 0) {
            fileName = args[0];
        }

        File recFile = new File(recs_path + fileName + ".dat");
        RecordingPlayer player = new RecordingPlayer(recFile);
        if (args.length > 1) {
            target = args[1];
        }
        player.setTargetInstrument(target);
        initEvent = player.getInit();

        String queueName = fileName;
        queueAppender = new ChronicleQueueAppender(queueName);
        if (args.length > 2) {
            for (String secId : args[2].split(",")) {
                queueAppender.interesting(Integer.valueOf(secId));
            }
        }
        play(player);
    }

    static long start = System.nanoTime();

    private static void play(RecordingPlayer player) {
        LogUtil.log("Loading " + fileName, start);
        start = System.nanoTime();
        OrderEvent current = null, prev;
        AbstractFileReader<LiveEvent> reader = player.getReader();
        while (player.isActive()) {
            try {
                LiveEvent next = reader.next();
                if (!(next instanceof OrderEvent) || !queueAppender.filter(((BookAtom) next).getSecurityId())) continue;
//                System.out.println(((OrderEvent) next).getTimestamps().getSequence() + " " + next);
                prev = current;
                current = (OrderEvent) next;
//                long toSleep = calcSleep(prev, current);
                player.pushEvent(reader.getCount(), current);
//                if (current.getId() == 6816519540921l)
//                    System.out.println(current);

                LeanQuote quote = LeanQuote.getQuote(current);
                boolean append = queueAppender.append(quote);
//                if (!append) toSleep = 0;
//                if (toSleep > 0)
//                    Thread.sleep(toSleep);
            } catch (Exception e) {
                log.error("wtf writing! " + current, e);
            }
        }
        LogUtil.log("Done loading " + queueAppender.getSize(), start);
    }

    static int maxSleep = 0;

    private static long calcSleep(AbstractEvent prev, AbstractEvent current) {
        if (current == null || prev == null) return 0;
        long diff = (current.getLatestTimestamp() - prev.getLatestTimestamp()) / 1_000_000L;
        return Math.max(0, Math.min(diff, maxSleep));
    }
}
