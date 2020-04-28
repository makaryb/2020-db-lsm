package ru.mail.polis.boriskin;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class Cell {

    private final ByteBuffer K;
    private final Value V;

    static final Comparator<Cell> COMPARATOR =
            Comparator.comparing((Cell cell) -> cell.K.asReadOnlyBuffer())
                    .thenComparing(cell -> cell.V);

    public Cell(final ByteBuffer K, final Value V) {
        this.K = K;
        this.V = V;
    }
}
