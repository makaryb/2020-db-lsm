package ru.mail.polis.boriskin;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

/**
 * Своя реализация {@link NewDAO} интерфейса {@link DAO}, используя одну из реализаций java.util.SortedMap.
 *
 * @author Makary Boriskin
 */
public final class NewDAO implements DAO {

    private final File base;
    private final long maxHeapThreshold;

    private Table memTable;
    private final Collection<SortedStringTable> ssTableCollection;

    private int gen;

    public NewDAO(final File base, final long maxHeapThreshold) throws IOException {
        this.base = base;
        assert maxHeapThreshold >= 0L;
        this.maxHeapThreshold = maxHeapThreshold;

        memTable = new MemTable();
        ssTableCollection = new ArrayList<SortedStringTable>();

        Files.walkFileTree(base.toPath(), EnumSet.of(FileVisitOption.FOLLOW_LINKS), 1,
                new SimpleFileVisitor<>() {
           @Override
           public FileVisitResult visitFile(final Path path,
                                            final BasicFileAttributes attributes) throws IOException {
               if (path.getFileName().toString().endsWith(".db")
                       && path.getFileName().toString().endsWith("SSTABLE")) {
                   ssTableCollection.add(new SortedStringTable(path.toFile()));
               }
               return FileVisitResult.CONTINUE;
           }
        });
        gen = ssTableCollection.size() - 1;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        // TODO:
        return null;
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        // TODO:
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        // TODO:
    }

    @Override
    public void close() throws IOException {
        // TODO:
    }
}
