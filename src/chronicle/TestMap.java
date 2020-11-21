package chronicle;

import events.LiveEvent;
import events.book.LeanQuote;
import events.book.BookAtom;
import events.feed.InitializeTradableEvent;
import events.feed.MarketOrder;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Values;
import org.apache.log4j.Logger;
import recorder.RecordingPlayer;
import utils.DateUtils;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

public class TestMap {
    private static Logger log = Logger.getLogger(TestMap.class);

    private static final long HOW_MANY = 1_000_000l;

    private static final int VALUE_LENGTH = 2;
    static long lowest = Long.MAX_VALUE;

    public static void log(String txt, long start) {
        long now = System.currentTimeMillis();
        long timeDiff = now - start;
        lowest = Math.min(lowest, timeDiff);
        log.info(String.format("Took (ms) %d / %d - %s", timeDiff, lowest, txt));
        //        log.info("time: " + DateUtils.fullTimeMilliFormat.format(now) + " took (ms): " + (now - start) + " \t- " + txt);
    }

    public static void logNano(String txt, long start) {
        long now = System.nanoTime();
        log.info("time: " + DateUtils.fullTimeMilliFormat.format(System.currentTimeMillis()) + " took (us): " + (int) ((now - start) / 1000) + " \t- " + txt);
    }

    static String fileName = "C:/Users/Mati/Documents/Player/liveRecordings/RTAlgorithms_200910_020726.dat";
    static String target = "F MINISP SEP20";

    public static void main(String[] args) {
        File path = new File("DB/chronicle");
        path.mkdirs();
        path = new File(path + "/" + fileName.split("/")[6]);
        if (path.exists()) path.delete();

        long start = System.currentTimeMillis();
        ChronicleMap<LongValue, BookAtom> map = null;
        try {
            map = ChronicleMapBuilder
                    .of(LongValue.class, BookAtom.class)
                    .name("map")
                    .entries(HOW_MANY)
                    .averageValue(new LeanQuote())
                    .createOrRecoverPersistedTo(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        LongValue key = Values.newHeapInstance(LongValue.class);

        File recFile = new File(fileName);
        RecordingPlayer player = new RecordingPlayer(recFile);

        player.setTargetInstrument(target);
        InitializeTradableEvent initEvent = player.getInit();
        int cnt = 0;
        log("Loading " + fileName, start);

        start = System.currentTimeMillis();
        while (player.isActive()) {
            LiveEvent next = player.next();

            if (!(next instanceof MarketOrder)) continue;
            LeanQuote order = LeanQuote.getQuote((MarketOrder) next);
            key.setValue(order.getId());
            map.put(key, order);
//                System.out.println(cnt++ + ": " + order);
        }
        log("Loaded " + map.longSize(), start);

        log("Start reading with iterator.", start);
        try {
            start = System.currentTimeMillis();

            Iterator<Entry<LongValue, BookAtom>> iterator = map.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<LongValue, BookAtom> order = iterator.next();
            }

            log("Done iterating. Deleting and inserting while iterating over map of: " + map.longSize(), start);

            start = System.currentTimeMillis();
            iterator = map.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<LongValue, BookAtom> order = iterator.next();
                map.remove(order.getKey());
                map.put(order.getKey(), order.getValue());
            }
            log("Done Deleting and reinserting: " + map.longSize(), start);
            start = System.currentTimeMillis();
            while (isRunning) {
                try {
                    Thread.sleep(100l);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            if (map != null) {
                map.close();
            }
        }

        if (path.exists()) path.delete();
    }

    static boolean isRunning = true;

    protected static int[] generateValueArray(long start) {
        int[] val = new int[VALUE_LENGTH];
        val[0] = (int) (start / 1000) + 1;
        val[1] = (int) (System.currentTimeMillis() / 1000);
        return val;
    }

}
