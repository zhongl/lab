package com.github.zhongl.store;

import com.google.common.io.Files;
import com.google.common.primitives.Ints;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link com.github.zhongl.store.Page} File structure :
 * <ul>
 * <p/>
 * <li>{@link com.github.zhongl.store.Page} = {@link com.github.zhongl.store.Item}* {@link com.github.zhongl.store.ItemIndex}* itemSize:Integer</li>
 * <li>{@link com.github.zhongl.store.Item} = Byte*</li>
 * <li>{@link com.github.zhongl.store.ItemIndex} = offset:Long length:Integer</li>
 * </ul>
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public class Page implements Closeable {
    public static final int ITEM_SIZE_BYTES = 4/* item size takes 4 bytes */;
    private final File file;
    private final long bytesCapacity;
    private final Appender appender;
    private final Getter getter;
    private final List<ItemIndex> itemIndices;

    private Page(File file, long bytesCapacity) throws IOException {
        this.file = file;
        this.bytesCapacity = bytesCapacity;
        itemIndices = new ArrayList<ItemIndex>();
        getter = new Getter();
        appender = new Appender();
    }

    public int itemSize() {
        return itemIndices.size();
    }

    public Appender appender() { return appender; }

    public Getter getter() { return getter; }

    public static Builder openOn(File file) { return new Builder(file); }

    public void close() throws IOException {
        appender.flush();
        appender.close();
        getter.close();
    }

    private abstract class Operator implements Closeable {
        protected final RandomAccessFile randomAccessFile;

        public Operator(RandomAccessFile randomAccessFile) {this.randomAccessFile = randomAccessFile;}

        @Override
        public void close() throws IOException {
            randomAccessFile.close();
        }
    }


    public class Appender extends Operator {

        private Appender() throws IOException {
            super(new RandomAccessFile(file, "rw"));
            if (itemSize() > 0) {
                seekFilePointerToEndOfLastItem();
            }
        }

        private void seekFilePointerToEndOfLastItem() throws IOException {
            ItemIndex lastItemIndex = itemIndices.get(itemSize() - 1);
            randomAccessFile.seek(lastItemIndex.offset() + lastItemIndex.length());
        }

        /**
         * Append item to the page.
         * <p/>
         * Caution: item will not sync to disk util {@link com.github.zhongl.store.Page.Appender#flush()} invoked.
         *
         * @param item {@link com.github.zhongl.store.Item}
         *
         * @return false if page has not enough capacity for new item, else true.
         * @throws IOException
         */
        public boolean append(Item item) throws IOException {
            if (overBytesCapacityWith(item.byteLength())) return false;

            long offset = randomAccessFile.getFilePointer();
            ItemIndex itemIndex = new ItemIndex(offset, item.byteLength());
            itemIndices.add(itemIndex);
            item.writeTo(randomAccessFile);
            return true;
        }

        /**
         * Sync pendings of {@link com.github.zhongl.store.Page} to disk.
         *
         * @throws IOException
         */
        public void flush() throws IOException {
            appendItemIndicesToFile();
            appendItemSizeToFile();
            flushToDisk();
            seekFilePointerToEndOfLastItem();
        }

        private boolean overBytesCapacityWith(int length) throws IOException {
            return randomAccessFile.getFilePointer() + length + 1L > bytesCapacity;
        }

        private void flushToDisk() throws IOException {
            randomAccessFile.getChannel().force(true);
        }

        private void appendItemSizeToFile() throws IOException {
            randomAccessFile.writeInt(itemSize());
        }

        private void appendItemIndicesToFile() throws IOException {
            for (ItemIndex itemIndex : itemIndices)
                itemIndex.writeTo(randomAccessFile);
        }

    }

    public class Getter extends Operator {

        private Getter() throws IOException {
            super(new RandomAccessFile(file, "r"));
            loadItemIndices();
        }

        public Item get(int index) throws IOException {
            ItemIndex itemIndex = itemIndices.get(index);
            randomAccessFile.seek(itemIndex.offset());
            return Item.readFrom(randomAccessFile, itemIndex.length());
        }

        private void loadItemIndices() throws IOException {
            int itemSize = readItemSize();
            int bytesOfItemIndices = ItemIndex.BYTES * itemSize;
            long beginOfItemIndices = randomAccessFile.length() - bytesOfItemIndices - ITEM_SIZE_BYTES;
            randomAccessFile.seek(beginOfItemIndices);
            for (int i = 0; i < itemSize; i++) {
                itemIndices.add(ItemIndex.readFrom(randomAccessFile));
            }
        }

        private int readItemSize() throws IOException {
            long beginOfItemSize = randomAccessFile.length() - ITEM_SIZE_BYTES;
            randomAccessFile.seek(beginOfItemSize);
            return randomAccessFile.readInt();
        }
    }

    public static class Builder {
        private static final long DEFAULT_BYTES_CAPACITY = 64 * 1024 * 1024; // 64M;

        private final File file;
        private long bytesCapacity = DEFAULT_BYTES_CAPACITY;

        private Builder(File file) { this.file = file; }

        public Builder createIfNotExist() throws IOException {
            if (file.exists()) return this;
            Files.createParentDirs(file);
            format();
            return this;
        }

        public Builder overwriteIfExist() throws IOException {
            if (file.exists()) format();
            return this;
        }

        public Page build() throws IOException {
            if (!file.exists()) {
                throw new IllegalArgumentException("Can't build a page on " + file
                        + ", because it does not exist.");
            }

            if (!file.isFile()) {
                throw new IllegalArgumentException(file + "exists but is not a file.");
            }

            return new Page(file, bytesCapacity);
        }

        public Builder bytesCapacity(long value) {
            bytesCapacity = value;
            return this;
        }

        private void format() throws IOException {
            Files.write(Ints.toByteArray(0), file); // write item size 0 to the file.
        }
    }
}
