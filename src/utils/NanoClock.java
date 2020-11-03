package utils;

import org.apache.commons.lang.SystemUtils;
import org.apache.log4j.Logger;
import setaffinitymanager.SetAffinityManager;

import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: mati
 * Date: 21/10/2020
 */
public class NanoClock {

    private static final Logger log = Logger.getLogger(NanoClock.class);

    private static long startNano, startMicros, nanoOffset;

    // PTP applicative synchronization
    static {
        long cNowMicros = 0;

        long firstMilli = System.currentTimeMillis();
        while (firstMilli == System.currentTimeMillis()) ; // Wait for the milli second change and then sync

        if (SystemUtils.IS_OS_LINUX) {
            try {
                cNowMicros = SetAffinityManager.getInstance().getCCurrentMicrosTime();
            } catch (Exception e) {
                log.error("set affinity jar or dll does not contain getCCurrentMillisTime. using java clock", e);
            }
        }
        startNano = System.nanoTime();
        startMicros = cNowMicros > 0 ? cNowMicros : (firstMilli + 1) * 1000;
        nanoOffset = TimeUnit.MICROSECONDS.toNanos(startMicros) - startNano;

        log.info("StartMilli=" + firstMilli + " / StartMicros= " + startMicros + " / nanoOffset= " + nanoOffset);
    }

    public static long getNanoTimeNow() {
        return nanoOffset + System.nanoTime();
    }

    public static void setOffset(long offset) {
        NanoClock.nanoOffset = offset;
    }
}
