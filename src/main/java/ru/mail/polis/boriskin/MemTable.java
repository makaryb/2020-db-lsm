package ru.mail.polis.boriskin;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public final class MemTable implements Table {

    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
    private long size;

    @Override
    public long getSize() {
        return size;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer point) throws IOException {
        return Iterators.transform(
          map.tailMap(point).entrySet().iterator(),
          e -> new Cell(e.getKey(), e.getValue()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer K, @NotNull ByteBuffer V) throws IOException {
        final Value prev = map.put(K, Value.valueOf(V));
        if (prev == null) {
            size += K.remaining() + V.remaining();
        } else if (prev.wasRemoved()) {
            size += V.remaining();
        } else {
            size += V.remaining() - prev.getData().remaining();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer K) throws IOException {
        final Value prev = map.put(K, Value.deadTest());
        if (prev == null) {
            size += K.remaining();
        } else if (!prev.wasRemoved()) {
            size -= prev.getData().remaining();
        }
    }
}
