package ru.mail.polis.boriskin;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class Cell {

    private final ByteBuffer K;
    private final Value V;

    static final Comparator<Cell> COMPARATOR = Comparator
            .comparing(Cell::getK)
            .thenComparing(Cell::getV);

    Cell(final ByteBuffer K, final Value V) {
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
