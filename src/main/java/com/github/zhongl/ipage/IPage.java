package com.github.zhongl.ipage;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class IPage implements Closeable, Iterable<Record> {

    private final File baseDir;
    private final int chunkCapcity;
    private final List<Chunk> chunks;
    private final AbstractList<Range> chunkOffsetRangeList;

    public static Builder baseOn(File dir) {
        return new Builder(dir);
    }

    private IPage(File baseDir, int chunkCapcity, List<Chunk> chunks) {
        this.baseDir = baseDir;
        this.chunkCapcity = chunkCapcity;
        this.chunks = chunks;
        chunkOffsetRangeList = new ChunkOfferRangeList();
    }

    public long append(Record record) throws IOException {
        try {
            return appendingChunk().append(record);
        } catch (OverflowException e) {
            newChunk();
            return append(record);
        }
    }

    private Chunk appendingChunk() throws IOException {
        if (chunks.isEmpty()) return newChunk();
        return chunks.get(0);
    }

    private Chunk newChunk() throws IOException {
        long beginPositionInIPage = chunks.isEmpty() ? 0L : appendingChunk().endPositionInIPage() + 1;
        Chunk chunk = new Chunk(beginPositionInIPage, new File(baseDir, beginPositionInIPage + ""), chunkCapcity);
        chunks.add(0, chunk);
        return chunk;
    }

    public Record get(long offset) throws IOException {
        if (chunks.isEmpty()) return null;
        return chunkIn(offset).get(offset);
    }

    private Chunk chunkIn(long offset) {
        int index = Range.binarySearch(chunkOffsetRangeList, offset);
        return chunks.get(index);
    }

    public void flush() throws IOException {
        appendingChunk().flush();
    }

    @Override
    public void close() throws IOException {
        appendingChunk().close();
    }

    @Override
    public Iterator<Record> iterator() {
        return null;  // TODO iterator
    }

    /**
     * Remove {@link Record} before the offset.
     *
     * @param offset
     */
    public void truncate(long offset) throws IOException {
        int index = Range.binarySearch(chunkOffsetRangeList, offset);
        Chunk toTruncateChunk = chunks.get(index);
        List<Chunk> toRmoved = chunks.subList(index + 1, chunks.size());
        for (Chunk chunk : toRmoved) {
            chunk.erase();
        }
        toRmoved.clear();
        chunks.add(toTruncateChunk.truncate(offset));
    }

    public static final class Builder {

        private static final int MIN_CHUNK_CAPACITY = 4096;
        private static final int UNSET = -1;
        private static final Pattern CHUNK_NAME_PATTERN = Pattern.compile("[0-9]+");
        private final File baseDir;
        private int chunkCapcity = UNSET;

        public Builder(File dir) {
            if (!dir.exists()) checkState(dir.mkdirs(), "Can not create directory: %s", dir);
            checkArgument(dir.isDirectory(), "%s should be a directory.", dir);
            baseDir = dir;
        }

        public Builder chunkCapacity(int value) {
            checkState(chunkCapcity == UNSET, "Chunk capacity can only set once.");
            checkArgument(value >= MIN_CHUNK_CAPACITY, "Chunk capacity should not less than %s", MIN_CHUNK_CAPACITY);
            chunkCapcity = value;
            return this;
        }

        public IPage build() throws IOException {
            chunkCapcity = chunkCapcity == UNSET ? MIN_CHUNK_CAPACITY : chunkCapcity;
            List<Chunk> chunks = validateAndLoadChunks();
            return new IPage(baseDir, chunkCapcity, chunks);
        }

        private List<Chunk> validateAndLoadChunks() throws IOException {
            String[] fileNames = baseDir.list(new ChunkFilter());
            Arrays.sort(fileNames, new ChunkNameComparator());

            ArrayList<Chunk> chunks = new ArrayList<Chunk>(fileNames.length);
            for (String fileName : fileNames) {
                // TODO validate chunks
                Chunk chunk = new Chunk(Long.parseLong(fileName), new File(baseDir, fileName), chunkCapcity);
                chunks.add(0, chunk); // reverse order to make sure the appending chunk at first.
            }
            return chunks;
        }

        private static class ChunkFilter implements FilenameFilter {
            @Override
            public boolean accept(File dir, String name) {
                return CHUNK_NAME_PATTERN.matcher(name).matches();
            }
        }

        private static class ChunkNameComparator implements Comparator<String> {
            @Override
            public int compare(String name1, String name2) {
                return (int) (Long.parseLong(name1) - Long.parseLong(name2));
            }

        }
    }

    private class ChunkOfferRangeList extends AbstractList<Range> {
        @Override
        public Range get(int index) {
            Chunk chunk = IPage.this.chunks.get(index);
            return new Range(chunk.beginPositionInIPage(), chunk.endPositionInIPage());
        }

        @Override
        public int size() {
            return IPage.this.chunks.size();
        }
    }
}
