import events.LiveEvent;
import events.book.LeanQuote;
import events.feed.InitializeTradableEvent;
import events.feed.MarketOrder;
import net.openhft.chronicle.ExcerptTailer;
import recorder.RecordingPlayer;

import java.io.File;
import java.io.IOException;

public class MainPlayer {
    public static String fileName = "C:/Users/Mati/Documents/Player/liveRecordings/RTAlgorithms_200910_020726.dat";
    static String target = "F MINISP SEP20";
    static InitializeTradableEvent initEvent;

    static ChronicleQueueAppender queue;

    public static void main(String[] args) {

        File recFile = new File(fileName);
        RecordingPlayer player = new RecordingPlayer(recFile);
        String recName = fileName.split("/")[6];
        recName = recName.split("\\.")[0];
        recName += "_" + System.currentTimeMillis();
        player.setTargetInstrument(target);
        initEvent = player.getInit();
        queue = new ChronicleQueueAppender(recName);

        play(player);
    }

    static long start = System.currentTimeMillis();

    private static void play(RecordingPlayer player) {
        TestMap.log("Loading " + fileName, start);
        start = System.currentTimeMillis();

        while (player.isActive()) {
            LiveEvent next = player.next();
            if (!(next instanceof MarketOrder)) continue;
            LeanQuote quote = TestMap.getQuote((MarketOrder) next);
            queue.append(quote);
            appended++;
        }

        TestMap.log("Replaying loaded " + queue.getSize(), start);
        start = System.currentTimeMillis();
        try (ExcerptTailer tailer = queue.createTailer()) {
            tailer.toStart();
            Object obj;
            do {
                obj = ChronicleTailer.getNext(tailer);
                tailed++;
//                System.out.println(cnt++ + ": " + obj);
            } while (obj != null);
        } catch (IOException e) {
            e.printStackTrace();
        }
        TestMap.log("Done tailing " + tailed, start);
    }

    static int appended = 0, tailed = 0;
}
