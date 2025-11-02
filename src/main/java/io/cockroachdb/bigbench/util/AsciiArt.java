package io.cockroachdb.bigbench.util;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class AsciiArt {
    private AsciiArt() {
    }

    public static String happy() {
        return "(ʘ‿ʘ)";
    }

    public static String bye() {
        return "(ʘ‿ʘ)╯";
    }

    public static String shrug() {
        return "¯\\_(ツ)_/¯";
    }

    public static String flipTableGently() {
        return "(╯°□°)╯︵ ┻━┻";
    }

    public static String flipTableRoughly() {
        return "(ノಠ益ಠ)ノ彡┻━┻";
    }

    private static final char[] TICKS = "|/-|\\-".toCharArray();

    private static final AtomicInteger t = new AtomicInteger();

    public static void tick(String prefix, int rows) {
        System.out.printf("\r%s (%s) %,6d", prefix, TICKS[t.incrementAndGet() % TICKS.length], rows);
    }

    public static void tock() {
        System.out.println();
    }
}

