package chronicle;

import events.AbstractEvent;
import events.LiveEvent;
import events.book.LeanQuote;
import events.feed.InitializeTradableEvent;
import events.feed.MarketOrder;
import events.feed.OrderEvent;
import events.rt.HitAndShadowEvent;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import recorder.RecordingPlayer;

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

    static long start = System.currentTimeMillis();

    private static void play(RecordingPlayer player) {
        TestMap.log("Loading " + fileName, start);
        start = System.currentTimeMillis();
        OrderEvent current = null, prev;
        while (player.isActive()) {
            try {
                LiveEvent next = player.next();
//                System.out.println(next);
                if (!(next instanceof OrderEvent)) continue;
                prev = current;
                current = (OrderEvent) next;
                LeanQuote quote = LeanQuote.getQuote(current);

                long toSleep = calcSleep(prev, current);
                if (!queueAppender.append(quote)) toSleep = 0;

                if (toSleep > 0)
                    Thread.sleep(toSleep);
            } catch (Exception e) {
                log.error("wtf writing! " + current, e);
            }
        }
        TestMap.log("Done loading " + queueAppender.getSize(), start);
    }

    static int maxSleep = 0;

    private static long calcSleep(AbstractEvent prev, AbstractEvent current) {
        if (current == null || prev == null) return 0;
        long diff = (current.getLatestTimestamp() - prev.getLatestTimestamp()) / 1_000_000L;
        return Math.max(0, Math.min(diff, maxSleep));
    }
}
