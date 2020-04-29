package ru.mail.polis.boriskin;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.Integer.*;
import static java.lang.Integer.BYTES;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.*;

public class SortedStringTable implements Table {
    private final long size;
    private final int rows;

    private final ByteBuffer cells;
    // хранит указатели на начало каждой строки
    private final IntBuffer offsets;

    @Override
    public long getSize() {
        return size;
    }

    @NotNull
    @Override
    public Iterator<TableCell> iterator(@NotNull ByteBuffer point) throws IOException {
        return new Iterator<TableCell>() {
            int next = findNext(point);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            // пользователь дергает next.
            // мы движемся по итераторам - мерджим их,
            // выбираем самое свежее значение - возвращаем его пользователю.
            // движемся дальше
            @Override
            public TableCell next() {
                assert hasNext();
                return findCell(next++);
            }
        };
    }

    private TableCell findCell(final int index) {
        if ((index < 0) || (index >= rows)) {
            throw new AssertionError();
        }

        int offset = offsets.get(index);

        /*
        Key Module
         */

        // Используем длину ключа
        final int sizeOfK = cells.getInt(offset);
        offset += BYTES;

        final ByteBuffer K = cells.duplicate();
        K.position(offset);
        K.limit(K.position() + sizeOfK);
        offset += sizeOfK;

        /*
        TimeStamp Module
         */
        final long timeStamp = cells.getLong(offset);
        offset += Long.BYTES;
        if (timeStamp < 0) {
            // если это могилка, то дальше ничего нет
            return new TableCell(K.slice(), new Value(-timeStamp, null));
        } else {
            /*
            Values Module
             */
            final int sizeOfV = cells.getInt(offset);
            offset += BYTES;

            final ByteBuffer V = cells.duplicate();
            V.position(offset);
            V.limit(V.position() + sizeOfV).position(offset).limit(offset + sizeOfV);

            // если это нормальное значение, то дальше длина этого значения и само значение
            return new TableCell(K.slice(), new Value(timeStamp, V.slice()));
        }
    }

    // бинарный поиск поверх файла
    private int findNext(final ByteBuffer point) {
        int l = 0;
        int r = rows - 1;
        while (l < r + 1) {
            // берем строчку n/2
            final int m = l + (r - l) / 2;
            // прыгаем по этой строке,
            // читаем ключ, сравниваем с тем, что пользователь передал
            final int cmp = findK(m).compareTo(point);
            // понимаем в какую сторону смотреть
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
        if ((index < 0) || (index >= rows)) {
            throw new AssertionError();
        }

        final int offset = offsets.get(index);
        final int sizeOfK = cells.getInt(offset);
        final ByteBuffer K = cells.duplicate();

        K.position(offset + BYTES);
        K.limit(K.position() + sizeOfK);

        return K.slice();
    }

    // Отсортированная таблица на диске.
    // После записи на диск поддерживает только операции чтения.
    SortedStringTable(final File f) throws IOException {
        this.size = f.length();
        if ((size == 0) || (size > MAX_VALUE)) {
            throw new AssertionError();
        }

        final MappedByteBuffer mapped;
        try (FileChannel fileChannel = FileChannel.open(f.toPath(), READ)) {
            mapped = (MappedByteBuffer) fileChannel.map(READ_ONLY, 0L, fileChannel.size()).order(BIG_ENDIAN);
        }

        rows = mapped.getInt((int) (size - BYTES));

        final ByteBuffer offsets = mapped.duplicate();
        final ByteBuffer cells = mapped.duplicate();

        offsets.position(mapped.limit() - BYTES * rows - BYTES);
        offsets.limit(mapped.limit() - BYTES);
        cells.limit(offsets.position());

        this.offsets = offsets.slice().asIntBuffer();
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

    static void writeMemTableDataToDisk(final Iterator<TableCell> cells, final File target) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(target.toPath(),
                CREATE_NEW,
                WRITE)) {
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (cells.hasNext()) {
                offsets.add(offset);

                final TableCell tableCell = cells.next();

                /*
                Key Module
                 */
                final ByteBuffer K = tableCell.getK();
                final int sizeOfK = tableCell.getK().remaining();

                fileChannel.write(Bytes.fromInt(sizeOfK));
                offset += BYTES;
                fileChannel.write(K);
                offset += sizeOfK;

                /*
                Value Module
                 */
                final Value V = tableCell.getV();

                /*
                TimeStamp Module
                храним монотонно увеличивающийся в системе Time Stamp,
                чтобы можно было взять строки и по значению версии определить что свежее
                 */
                if (V.wasRemoved()) {
                    fileChannel.write(Bytes.fromLong(-tableCell.getV().getTimeStamp()));
                } else {
                    fileChannel.write(Bytes.fromLong(tableCell.getV().getTimeStamp()));
                }

                offset += Long.BYTES;

                /*
                back to Value Module
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
            Offsets Module
             */
            for (final Integer o : offsets) {
                fileChannel.write(Bytes.fromInt(o));
            }

            /*
            Cells Module
             */
            fileChannel.write(Bytes.fromInt(offsets.size()));
        }
    }
}
