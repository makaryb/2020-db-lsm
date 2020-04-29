package ru.mail.polis.boriskin;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {

    // либо ByteBuffer, либо могилка + Time Stamp (версия)
    private final long timeStamp;
    private final ByteBuffer data;

    @Override
    public int compareTo(@NotNull Value V) {
        return -Long.compare(timeStamp, V.timeStamp);
    }

    Value(final long timeStamp, final ByteBuffer data) {
        this.timeStamp = timeStamp;
        this.data = data;
    }

    public static Value valueOf(final ByteBuffer data) {
        // по рекомендации из лекции в качестве значения версии
        // используется отметка времени
        return new Value(Utils.getTime(), data.duplicate());
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public ByteBuffer getData() {
        if (data == null) {
            throw new IllegalArgumentException();
        }
        return data.asReadOnlyBuffer();
    }

    static Value tombstone() {
        // у могилки есть версия - тот же Time Stamp
        return new Value(Utils.getTime(), null);
    }

    boolean wasRemoved() {
        return data == null;
    }
}
