package ru.mail.polis.boriskin;

import java.nio.ByteBuffer;
import java.util.Comparator;

final class TableCell {

    private final ByteBuffer key;
    private final Value val;

    static final Comparator<TableCell> COMPARATOR = Comparator
            .comparing(TableCell::getK)
            .thenComparing(TableCell::getV);

    TableCell(final ByteBuffer key, final Value val) {
        this.key = key;
        this.val = val;
    }

    public ByteBuffer getK() {
        return key.asReadOnlyBuffer();
    }

    public Value getV() {
        return val;
    }
}
