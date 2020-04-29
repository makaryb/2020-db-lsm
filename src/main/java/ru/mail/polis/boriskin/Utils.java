package ru.mail.polis.boriskin;

final class Utils {
    private static long time;
    // обогащение
    private static int counter;

    private Utils() {
        // do nothing
    }

    static long getTime() {
        final long currentTime = System.nanoTime();
        if (currentTime != time) {
            time = currentTime;
            counter = 0;
        }
        return currentTime + counter++;
    }
}
