import events.LiveEvent;
import events.book.LeanQuote;
import events.feed.InitializeTradableEvent;
import events.feed.MarketOrder;
import net.openhft.chronicle.ExcerptTailer;
import org.apache.log4j.Logger;
import recorder.RecordingPlayer;

import java.io.File;
import java.io.IOException;

public class MainPlayer {
    private static Logger log = Logger.getLogger(MainPlayer.class);

    public static String recsPath = "C:/Users/Mati/Documents/Player/liveRecordings/";
    public static String fileName = "RTAlgorithms_200910_020726";
    static String target = "F MINISP SEP20";
//    public static String fileName = "RTAlgorithms_200902_094745";
//    static String target = "F DAX SEP20";

    static InitializeTradableEvent initEvent;

    static ChronicleQueueAppender queue;

    public static void main(String[] args) {

        File recFile = new File(recsPath + fileName + ".dat");
        RecordingPlayer player = new RecordingPlayer(recFile);
        String queueName = fileName;
//        queueName += "_" + System.currentTimeMillis();
        player.setTargetInstrument(target);
        initEvent = player.getInit();
        queue = new ChronicleQueueAppender(queueName);

        play(player);
    }

    static long start = System.currentTimeMillis();

    private static void play(RecordingPlayer player) {
        TestMap.log("Loading " + fileName, start);
        start = System.currentTimeMillis();
        LiveEvent next = null;
        while (player.isActive()) {
            try {
                next = player.next();
                if (!(next instanceof MarketOrder)) continue;
                LeanQuote quote = LeanQuote.getQuote((MarketOrder) next);
                queue.append(quote);
                appended++;
//                System.out.println(appended + " " + quote);
            } catch (Exception e) {
                log.error("wtf" + next, e);
            }
        }

        TestMap.log("Done loading " + queue.getSize(), start);
        start = System.currentTimeMillis();
        try (ExcerptTailer tailer = queue.createTailer()) {
            tailer.toStart();
            Object obj;
            do {
                obj = ChronicleTailer.getNext(tailer);
                tailed++;
//                System.out.println(tailed + ": " + obj);
            } while (obj != null);
        } catch (IOException e) {
            e.printStackTrace();
        }
        TestMap.log("Done tailing " + tailed, start);
    }

    static int appended = 0, tailed = 0;
}
