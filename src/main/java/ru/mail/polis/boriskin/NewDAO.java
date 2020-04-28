package ru.mail.polis.boriskin;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
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
    public Iterator<Record> iterator(@NotNull final ByteBuffer point) throws IOException {
        final Collection<Iterator<Cell>> filesIterator = new ArrayList<>();

        /*
        SSTables iterators
         */
        for (final SortedStringTable sortedStringTable : ssTableCollection) {
            filesIterator.add(sortedStringTable.iterator(point));
        }

        /*
        MemTable iterator
         */
        filesIterator.add(memTable.iterator(point));
        final Iterator<Cell> cells = Iters.collapseEquals(
                Iterators.mergeSorted(filesIterator, Cell.COMPARATOR),
                Cell::getK);
        final Iterator<Cell> transforms = Iterators.filter(cells,
                cell -> !cell.getV().wasRemoved());

        return Iterators.transform(transforms,
                cell -> Record.of(cell.getK(), cell.getV().getData()));

    }

    @Override
    public void upsert(@NotNull final ByteBuffer K, @NotNull final ByteBuffer V) throws IOException {
        memTable.upsert(K, V);
        if (memTable.getSize() >= maxHeapThreshold) {
            close();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer K) throws IOException {
        memTable.remove(K);
        if (memTable.getSize() >= maxHeapThreshold) {
            close();
        }
    }

    @Override
    public void close() throws IOException {
        final File temp = new File(base, "SSTABLE" + gen + ".tmp");
        SortedStringTable.writeMemTableDataToDisk(
                memTable.iterator(ByteBuffer.allocate(0)),
                temp);
        final File dest = new File(base, "SSTABLE" + gen + ".db");
        Files.move(temp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        gen++;
        memTable = new MemTable();
    }
}
