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

import static java.nio.file.FileVisitOption.*;
import static java.nio.file.FileVisitResult.*;
import static java.nio.file.StandardCopyOption.*;

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

    // счетчик поколений
    private int gen;

    public NewDAO(final File base, final long maxHeapThreshold) throws IOException {
        this.base = base;
        if (maxHeapThreshold < 0L) {
            throw new AssertionError();
        }
        this.maxHeapThreshold = maxHeapThreshold;

        memTable = new MemTable();
        ssTableCollection = new ArrayList<SortedStringTable>();

        // сканируем иерархию в папке с целью понять, что там лежат SSTable'ы
        Files.walkFileTree(base.toPath(), EnumSet.of(FOLLOW_LINKS), 1,
                new SimpleFileVisitor<>() {
           @Override
           public FileVisitResult visitFile(final Path path,
                                            final BasicFileAttributes attributes) throws IOException {
               if (path.getFileName().toString().endsWith(".db")
                       && path.getFileName().toString().startsWith("SortedStringTABLE")) {
                   ssTableCollection.add(new SortedStringTable(path.toFile()));
               }
               return CONTINUE;
           }
        });

        // более свежая версия из того, что лежит на диске
        // (0 - старая, size()-1 - самая новая)
        gen = ssTableCollection.size() - 1;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer point) throws IOException {
        final Collection<Iterator<TableCell>> filesIterator = new ArrayList<>();

        /*
        SSTables iterators Module
         */
        for (final SortedStringTable sortedStringTable : ssTableCollection) {
            filesIterator.add(sortedStringTable.iterator(point));
        }

        /*
        MemTable iterator Module
         */
        filesIterator.add(memTable.iterator(point));
        // итератор мерджит разные потоки и выбирает самое актуальное значение
        final Iterator<TableCell> cells = Iters.collapseEquals(
                Iterators.mergeSorted(filesIterator, TableCell.COMPARATOR),
                TableCell::getK);
        // может быть "живое" значение, а может быть, что значение по ключу удалили в момент времени Time Stamp
        final Iterator<TableCell> alive = Iterators.filter(cells,
                cell -> !cell.getV().wasRemoved());
        // после мерджа ячеек разных таблиц,
        // при возвращении итератора пользователю:
        // в этот момент превращает их в рекорды (transform)
        return Iterators.transform(alive,
                cell -> Record.of(cell.getK(), cell.getV().getData()));
    }

    // вставить-обновить
    @Override
    public void upsert(@NotNull final ByteBuffer K, @NotNull final ByteBuffer V) throws IOException {
        memTable.upsert(K, V);
        // когда размер таблицы достигает порога,
        // сбрасываем данную таблицу на диск,
        // где она хранится в бинарном сериализованном виде
        if (memTable.getSize() >= maxHeapThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer K) throws IOException {
        memTable.remove(K);
        // сбрасываем таблицу на диск
        if (memTable.getSize() >= maxHeapThreshold) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        // сохранить все, что мы не сохранили
        flush();
    }

    private void flush() throws IOException {
        // в начале нужно писать во временный файл
        final File temp = new File(base, "SortedStringTABLE" + gen + ".tmp");

        SortedStringTable.writeMemTableDataToDisk(
                memTable.iterator(ByteBuffer.allocate(0)),
                temp);

        // превращаем в постоянный файл
        final File dest = new File(base, "SortedStringTABLE" + gen + ".db");
        Files.move(temp.toPath(), dest.toPath(), ATOMIC_MOVE);

        // обновляем счетчик поколений
        gen++;
        // заменяем MemTable в памяти на пустой
        memTable = new MemTable();
        // таким образом, на диске копятся SSTable'ы + есть пустой-непустой MemTable в памяти
    }
}
