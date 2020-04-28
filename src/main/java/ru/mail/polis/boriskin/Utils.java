package ru.mail.polis.boriskin;

final class Utils {
    private static long time;
    private static int counter;

    private Utils() {}

    static long getTime() {
        final long currentTime = System.currentTimeMillis();
        if (currentTime != time) {
            time = currentTime;
            counter = 0;
        }
        return toNanoSeconds(currentTime) + counter++;
    }

    private static long toNanoSeconds(final long currentTime) {
        return currentTime * 1_000_000;
    }
}
