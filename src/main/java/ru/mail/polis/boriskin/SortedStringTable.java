package ru.mail.polis.boriskin;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.Integer.BYTES;
import static java.lang.Integer.MAX_VALUE;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.*;

public class SortedStringTable implements Table {
    private final long size;
    private int rows;

    private ByteBuffer cells;
    private IntBuffer offsets;

    @Override
    public long getSize() {
        return size;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer point) throws IOException {
        return new Iterator<Cell>() {
            int next = findNext(point);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cell next() {
                assert hasNext();
//                TODO:
//                return findCell(next++);
                return null;
            }
        };
    }

    private int findNext(ByteBuffer point) {
        int l = 0;
        int r = rows - 1;
        while (l <= r) {
            final int m = l + (r - l) / 2;
            final int cmp = findK(m).compareTo(point);
            if (cmp < 0) {
                l = m + 1;
            } else if (cmp > 0) {
                r = m - 1;
            } else {
                return m;
            }
        }
        return l;
    }

    private Comparable<ByteBuffer> findK(int index) {
        assert 0 <= index && index < rows;

        final int offset = offsets.get(index);
        final int sizeOfK = cells.getInt(offset);
        final ByteBuffer K = cells.duplicate();

        K.position(K.position() + sizeOfK);

        return K.slice();
    }

    SortedStringTable(final File f) throws IOException {
        this.size = f.length();
        if ((size == 0)
                || (size > MAX_VALUE)) {
            throw new AssertionError();
        }

        final ByteBuffer mapped;
        try (FileChannel fileChannel = FileChannel.open(f.toPath(), READ)) {
            mapped = fileChannel.map(READ_ONLY, 0L, fileChannel.size()).order(BIG_ENDIAN);
        }

        /*
        Rows
         */
        rows = mapped.getInt((int) (size - BYTES));

        /*
        Offset
         */
        final ByteBuffer offsets = mapped.duplicate();
        offsets.position(mapped.limit() - BYTES * rows - BYTES);
        offsets.limit(mapped.limit() - BYTES);
        this.offsets = offsets.slice().asIntBuffer();

        /*
        Cells
         */
        final ByteBuffer cells = mapped.duplicate();
        cells.limit(offsets.position());
        this.cells = cells.slice();
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
                CREATE_NEW,
                WRITE)) {
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (cells.hasNext()) {
                offsets.add(offset);
                final Cell cell = cells.next();

                /*
                Key
                 */
                final ByteBuffer K = cell.getK();
                final int sizeOfK = cell.getK().remaining();

                fileChannel.write(Bytes.fromInt(sizeOfK));
                offset += BYTES;
                fileChannel.write(K);
                offset += sizeOfK;

                /*
                Value
                 */
                final Value V = cell.getV();

                /*
                TimeStamp
                 */
                if (V.wasRemoved()) {
                    fileChannel.write(Bytes.fromLong(-cell.getV().getTimeStamp()));
                } else {
                    fileChannel.write(Bytes.fromLong(cell.getV().getTimeStamp()));
                }
                offset += Long.BYTES;

                /*
                back to Value
                 */
                if (!V.wasRemoved()) {
                    final ByteBuffer data = V.getData();
                    final int sizeOfV = V.getData().remaining();

                    fileChannel.write(Bytes.fromInt(sizeOfV));
                    offset += BYTES;
                    fileChannel.write(data);
                    offset += sizeOfV;
                }
            }

            /*
            Offsets
             */
            for (final Integer o : offsets) {
                fileChannel.write(Bytes.fromInt(o));
            }

            /*
            Cells
             */
            fileChannel.write(Bytes.fromInt(offsets.size()));
        }
    }
}
