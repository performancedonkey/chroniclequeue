package utils;

import org.apache.log4j.Logger;

public class LogUtil {
    private static final Logger log = Logger.getLogger(LogUtil.class);
    static long lowest = Long.MAX_VALUE;

    public static void log(String txt, long start, long processed) {
        long now = System.nanoTime();
        long timeDiff = (now - start) / 1_000_000;
        lowest = Math.min(lowest, timeDiff);
        int kmps = (int) (processed / timeDiff);
        log.error(String.format("Took (ms) %d / %d - %s batch of %d | kmps:  %d", timeDiff, lowest, txt, processed, kmps));
    }

    public static void log(String txt, long start) {
        long now = System.nanoTime();
        long timeDiff = (now - start) / 1_000_000;
        lowest = Math.min(lowest, timeDiff);
        log.error(String.format("Took (ms) %d / %d - %s", timeDiff, lowest, txt));
        //        log.info("time: " + DateUtils.fullTimeMilliFormat.format(now) + " took (ms): " + (now - start) + " \t- " + txt);
    }

}
