package ru.mail.polis.boriskin;

import java.nio.ByteBuffer;

final class Bytes {
    Bytes() {}

    static ByteBuffer fromInt(final int value) {
        final ByteBuffer res = ByteBuffer.allocate(Integer.BYTES);
        res.putInt(value);
        res.rewind();
        return res;
    }

    static ByteBuffer fromLong(final long value) {
        final ByteBuffer res = ByteBuffer.allocate(Long.BYTES);
        res.putLong(value);
        res.rewind();
        return res;
    }
}
