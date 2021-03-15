package no.simula.umod.redditgraph;

import java.text.SimpleDateFormat;
import java.util.Calendar;

class ConsoleUtils {

    /**
     * Logg the message with including the time
     * @param message message to log
     */
    public static void log(final Object message){
        System.out.println(getDateString() + "  " + message + "  " + getMemoryString());
    }

    /**
     * Logg the message including the time and duration
     *
     * @param message message to log
     * @param startNanoTime start time in nano seconds (usually from System.nanoTime())
     */
    public static void logDuration(final Object message, final long startNanoTime) {

        System.out.println(getDateString() + "  " + message + " (" + getReadableDuration(startNanoTime) + ")  " + getMemoryString());
    }

    /** Get nicely formatted string of the current time */
    private static String getDateString() {
        final var date = Calendar.getInstance().getTime();
        final var dateFormat = new SimpleDateFormat("MMM dd HH:mm:ss");
        return dateFormat.format(date);
    }

    /** Get JVM memory usage as a short string */
    private static String getMemoryString() {
        final int gb = 1024*1024*1024;
        final var runtime = Runtime.getRuntime();
        final int usedMemory = (int) ((runtime.totalMemory() - runtime.freeMemory()) / gb);
        final int totalMemory = (int) (runtime.totalMemory() / gb);
        final int maxMemory = (int) (runtime.maxMemory() / gb);
        final var memory =
                "U:" + usedMemory  +
                "T:" + totalMemory +
                "M:" + maxMemory;
        return memory;
    }

    /**
     * Prints a duration string from nano time
     * @param startNanoTime start time
     * @return e.g 1d 14h 12m 44s
     * @apiNote source: https://stackoverflow.com/a/45075606
     */
    private static String getReadableDuration(long startNanoTime){
        final long duration = (long) ((System.nanoTime() - startNanoTime) / 1e9d);

        final long sec        = duration % 60;
        final long min        = (duration /60) % 60;
        final long hour       = (duration /(60*60)) % 24;
        final long day        = (duration / (24*60*60)) % 24;

        return String.format("%dd %dh %dm %ds", day,hour,min,sec);

    }
}

