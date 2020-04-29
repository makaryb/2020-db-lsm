package ru.mail.polis.boriskin;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

// В данном задании реализация осуществляется
// согласно структуре, представленной в конце лекции
public interface Table {

    long getSize();

    @NotNull
    Iterator<Cell> iterator(@NotNull ByteBuffer point) throws IOException;

    void upsert(@NotNull ByteBuffer K, @NotNull ByteBuffer V) throws IOException;

    void remove(@NotNull ByteBuffer K) throws IOException;
}