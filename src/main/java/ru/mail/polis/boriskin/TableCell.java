package ru.mail.polis.boriskin;

import java.nio.ByteBuffer;
import java.util.Comparator;

final class TableCell {

    private final ByteBuffer K;
    private final Value V;

    static final Comparator<TableCell> COMPARATOR = Comparator
            .comparing(TableCell::getK)
            .thenComparing(TableCell::getV);

    TableCell(final ByteBuffer K, final Value V) {
        this.K = K;
        this.V = V;
    }

    public ByteBuffer getK() {
        return K.asReadOnlyBuffer();
    }

    public Value getV() {
        return V;
    }
}
