package no.simula.umod.redditgraph;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class ConsoleUtils {

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
        final long duration = (long) ((System.nanoTime() - startNanoTime) / 1e9d);

        System.out.println(getDateString() + "  " + message + " (" + getReadableTime(duration) + ")  " + getMemoryString());
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
     * Prints a string from nano time
     * @param nanos
     * @return
     * @source: https://stackoverflow.com/a/45075606
     */
    private static String getReadableTime(Long nanos){

        long tempSec    = nanos/(1000*1000*1000);
        long sec        = tempSec % 60;
        long min        = (tempSec /60) % 60;
        long hour       = (tempSec /(60*60)) % 24;
        long day        = (tempSec / (24*60*60)) % 24;

        return String.format("%dd %dh %dm %ds", day,hour,min,sec);

    }
}

