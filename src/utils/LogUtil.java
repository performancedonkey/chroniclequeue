package utils;

import chronicle.ChronicleLiveTailer;

public class LogUtil {
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(ChronicleLiveTailer.class);
    static long lowest = Long.MAX_VALUE;

    public static void log(String txt, long start, long processed) {
        long now = System.currentTimeMillis();
        long timeDiff = now - start;
        lowest = Math.min(lowest, timeDiff);
        int kmps = (int) (processed / timeDiff);
        log.error(String.format("Took (ms) %d / %d - %s batch of %d | kmps:  %d", timeDiff, lowest, txt, processed, kmps));
    }

    public static void log(String txt, long start) {
        long now = System.currentTimeMillis();
        long timeDiff = now - start;
        lowest = Math.min(lowest, timeDiff);
        log.error(String.format("Took (ms) %d / %d - %s", timeDiff, lowest, txt));
        //        log.info("time: " + DateUtils.fullTimeMilliFormat.format(now) + " took (ms): " + (now - start) + " \t- " + txt);
    }

}
