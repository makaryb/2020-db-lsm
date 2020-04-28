package ru.mail.polis.boriskin;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {

    private final long timeStamp;
    private final ByteBuffer data;

    @Override
    public int compareTo(@NotNull Value V) {
        return Long.compare(timeStamp, V.timeStamp);
    }

    Value(final long timeStamp, final ByteBuffer data) {
        this.timeStamp = timeStamp;
        this.data = data;
    }

    public static Value valueOf(final ByteBuffer data) {
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


    static Value deadTest() {
        return new Value(Utils.getTime(), null);
    }

    boolean wasRemoved() {
        return data == null;
    }
}
