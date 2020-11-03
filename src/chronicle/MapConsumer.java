package chronicle;

;import events.book.LeanQuote;
import events.book.Manageable;
import events.feed.MarketOrder;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

public class MapConsumer {

    private static final long HOW_MANY = 1_000_000l;

    private static final int VALUE_LENGTH = 2;

    private static void log(String txt, long start) {
        long now = System.currentTimeMillis();
        System.out.println("time:" + now + " diff:" + (now - start) + " - " + txt);
    }

    public static void main(String[] args) {
        File path = new File("DB/chronicle");
        path.mkdirs();


        path = new File("DB/chronicle/testFile");
        if (path.exists()) path.delete();

        ChronicleMap<LongValue, Manageable> map = null;
        try {
            map = ChronicleMapBuilder
                    .of(LongValue.class, Manageable.class)
                    .name("map")
                    .entries(HOW_MANY)
                    .averageValue(new LeanQuote())
                    .createOrRecoverPersistedTo(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            log(i + "# Start iterating over " + map.longSize(), start);
            start = System.currentTimeMillis();
            iterateOverMap(map);
        }

        log("Done loading from memory: " + map.longSize(), start);

        if (path.exists()) path.delete();
        if (map != null) {
            map.close();
        }
    }

    private static void iterateOverMap(ChronicleMap<LongValue, Manageable> map) {

        Iterator<Entry<LongValue, Manageable>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<LongValue, Manageable> order = iterator.next();
//                System.out.println(order.getValue());
        }
    }

    private static LeanQuote getQuote(MarketOrder order) {
        LeanQuote leanQuote = new LeanQuote();

        leanQuote.setSecurityId(order.initEvent.tradableId);
        leanQuote.set(order.arrivalTime);
        leanQuote.set(order.xTimestamp);
        leanQuote.set(order.type.getQuoteType(),
                order.getSide(), order.price, order.amount, order.orderIdL);
        leanQuote.setLayer(order.layer);

        return leanQuote;
    }

    protected static int[] generateValueArray(long start) {
        int[] val = new int[VALUE_LENGTH];
        val[0] = (int) (start / 1000) + 1;
        val[1] = (int) (System.currentTimeMillis() / 1000);
        return val;
    }

}
