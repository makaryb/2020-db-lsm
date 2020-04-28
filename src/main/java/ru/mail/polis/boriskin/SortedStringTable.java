package ru.mail.polis.boriskin;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class SortedStringTable implements Table {
    private final long size;

    @Override
    public long getSize() {
        return size;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer point) throws IOException {
        return new Iterator<Cell>() {

            // TODO:

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Cell next() {
                return null;
            }
        };
    }

    SortedStringTable(final File f) throws IOException {
        this.size = f.length();
        assert size != 0
                && size <= Integer.MAX_VALUE;

        final ByteBuffer mapped;
        try (FileChannel fileChannel = FileChannel.open(f.toPath(), StandardOpenOption.READ)) {
            // TODO: BIG_ENDIAN
        }

        /*
        Rows
         */
        // TODO:

        /*
        Offset
         */
        // TODO:

        /*
        Cells
         */
        // TODO:
    }

    @Override
    public void upsert(@NotNull ByteBuffer K, @NotNull ByteBuffer V) throws IOException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void remove(@NotNull ByteBuffer K) throws IOException {
        throw new UnsupportedOperationException("");
    }

    static void writeMemTableDataToDisk(final Iterator<Cell> cells, final File target) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(target.toPath(),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            // TODO:
        }
    }
}
