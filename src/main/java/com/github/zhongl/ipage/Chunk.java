package com.github.zhongl.ipage;

import com.google.common.io.Files;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.util.Iterator;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * {@link com.github.zhongl.ipage.Chunk} File structure :
 * <ul>
 * <p/>
 * <li>{@link com.github.zhongl.ipage.Chunk} = {@link com.github.zhongl.ipage.Item}* </li>
 * <li>{@link com.github.zhongl.ipage.Item} = length:4bytes bytes</li>
 * </ul>
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
class Chunk implements Closeable {

    private final File file;
    private final long capacity;
    private final long beginPositionInIPage;

    private volatile MappedByteBuffer mappedByteBuffer;
    private volatile int writePosition = 0;

    /**
     * @param beginPositionInIPage
     * @param file
     * @param capacity
     *
     * @throws IOException
     */
    Chunk(long beginPositionInIPage, File file, long capacity) throws IOException {
        this.beginPositionInIPage = beginPositionInIPage;
        this.file = file;
        this.capacity = capacity;
        this.writePosition = (int) file.length();
    }

    public long append(Item item) throws IOException {
        checkOverFlowIfAppend(item.length());
        long iPageOffset = writePosition + beginPositionInIPage;
        ensureMap();
        mappedByteBuffer.position(writePosition);
        writePosition += item.writeTo(mappedByteBuffer.duplicate());
        return iPageOffset;
    }

    public Item get(long offset) throws IOException {
        ensureMap();
        mappedByteBuffer.position((int) (offset - beginPositionInIPage));
        return Item.readFrom(mappedByteBuffer.duplicate()); // duplicate to avoid modification of mappedDirectBuffer .
    }

    @Override
    public void close() throws IOException {
        if (mappedByteBuffer != null) {
            flush();
            DirectByteBufferCleaner.clean(mappedByteBuffer);
            mappedByteBuffer = null;
            trim();
        }
    }

    public long endPositionInIPage() {
        return beginPositionInIPage + writePosition - 1;
    }

    public long beginPositionInIPage() {
        return beginPositionInIPage;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Chunk");
        sb.append("{file=").append(file);
        sb.append(", capacity=").append(capacity);
        sb.append(", beginPositionInIPage=").append(beginPositionInIPage);
        sb.append(", writePosition=").append(writePosition);
        sb.append('}');
        return sb.toString();
    }

    public void flush() throws IOException {
        mappedByteBuffer.force();
    }

    public Iterator<Item> iterator() {
        return null;  // TODO iterator
    }

    private void checkOverFlowIfAppend(int length) {
        int appendedPosition = writePosition + length + Item.LENGTH_BYTES;
        if (appendedPosition > capacity) throw new OverflowException();
    }

    private void ensureMap() throws IOException {
        // TODO maybe a closed state is needed for prevent remap
        if (mappedByteBuffer == null) mappedByteBuffer = Files.map(file, READ_WRITE, capacity);
    }

    /**
     * Trim for keeping write position when closed.
     *
     * @throws IOException
     */
    private void trim() throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.setLength(writePosition);
        randomAccessFile.close();
    }
}
