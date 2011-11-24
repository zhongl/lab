package com.github.zhongl.store;

import com.google.common.io.Files;
import com.google.common.primitives.Longs;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

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
    public static final int SKIP_CAPACITY_BYTES = 8;
    private final File file;
    private final long bytesCapacity;
    private final Appender appender;
    private final Getter getter;

    private Page(File file, long bytesCapacity) throws IOException {
        this.file = file;
        this.bytesCapacity = bytesCapacity;
        getter = new Getter();
        appender = new Appender();
    }

    public Appender appender() {
        return appender;
    }

    public Getter getter() {
        return getter;
    }

    public static Builder openOn(File file) {
        return new Builder(file);
    }

    public void close() throws IOException {
        appender.close();
        getter.close();
    }

    private abstract class Operator implements Closeable {
        protected final RandomAccessFile randomAccessFile;

        public Operator(RandomAccessFile randomAccessFile) {
            this.randomAccessFile = randomAccessFile;
        }

        @Override
        public void close() throws IOException {
            randomAccessFile.close();
        }
    }


    public class Appender extends Operator {

        private Appender() throws IOException {
            super(new RandomAccessFile(file, "rw"));
            if (randomAccessFile.length() < bytesCapacity) {
                randomAccessFile.seek(randomAccessFile.length());
            }
        }

        /**
         * Append item to the page.
         * <p/>
         * Caution: item will not sync to disk util {@link com.github.zhongl.store.Page.Appender#flush()} invoked.
         *
         * @param item {@link Item}
         *
         * @return offset of the {@link com.github.zhongl.store.Item}.
         * @throws OverflowException if no remains for new item.
         * @throws IOException
         */
        public long append(Item item) throws IOException {
            checkOverFlowIfAppend(item.length());
            long offset = randomAccessFile.getFilePointer() - SKIP_CAPACITY_BYTES;
            item.writeTo(randomAccessFile);
            return offset;
        }

        /**
         * Sync pendings of {@link com.github.zhongl.store.Page} to disk.
         *
         * @throws IOException
         */
        public void flush() throws IOException {
            randomAccessFile.getChannel().force(true);
        }

        private void checkOverFlowIfAppend(int length) throws IOException {
            long appendedLength = randomAccessFile.getFilePointer() + length + Item.LENGTH_BYTES;
            if (appendedLength > bytesCapacity) throw new OverflowException();
        }

    }

    public class Getter extends Operator {

        private Getter() throws IOException {
            super(new RandomAccessFile(file, "r"));
        }

        public Item get(long offset) throws IOException {
            seek(offset);
            return Item.readFrom(randomAccessFile);
        }

        private void seek(long offset) throws IOException {
            randomAccessFile.seek(SKIP_CAPACITY_BYTES + offset);
        }
    }

    public static class Builder {
        public static final long DEFAULT_BYTES_CAPACITY = 64 * 1024 * 1024; // 64M;

        private final File file;

        private long bytesCapacity = DEFAULT_BYTES_CAPACITY;
        private boolean create = false;
        private boolean overwrite = false;

        private Builder(File file) {
            this.file = file;
        }

        public Builder createIfNotExist() {
            create = true;
            return this;
        }

        public Builder overwriteIfExist() {
            overwrite = true;
            return this;
        }

        public Page build() throws IOException {
            if (file.exists()) {
                if (overwrite) format();
            } else {
                if (!create)
                    throw new IllegalArgumentException("Can't build a page on " + file
                            + ", because it does not exist.");
                Files.createParentDirs(file);
                format();
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
            Files.write(Longs.toByteArray(bytesCapacity), file); // write item size 0 to the file.
        }
    }
}
