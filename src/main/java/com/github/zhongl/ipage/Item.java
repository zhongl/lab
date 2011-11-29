package com.github.zhongl.ipage;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * {@link Item} is wrapper of bytes, it provids read and write operation.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@ThreadSafe
public final class Item {
    public static final int LENGTH_BYTES = 4;
    private final ByteBuffer buffer;
    @Deprecated
    private final Md5Key md5Key;

    public static Item readFrom(ByteBuffer byteBuffer) throws IOException {
        int length = byteBuffer.getInt();
        int limit = byteBuffer.position() + length;
        byteBuffer.limit(limit);
        return new Item(byteBuffer.slice());
    }

    public Item(ByteBuffer buffer) {
        this.buffer = buffer;
        if (buffer.isDirect()) {
            byte[] bytes = new byte[buffer.limit()];
            buffer.get(bytes);
            md5Key = Md5Key.valueOf(bytes);
        } else {
            md5Key = Md5Key.valueOf(buffer.array());
        }
    }

    public Item(byte[] bytes) {
        md5Key = Md5Key.valueOf(bytes);
        buffer = ByteBuffer.wrap(bytes);
    }

    public int length() {
        return buffer.limit();
    }

    @Deprecated
    public Md5Key md5Key() {
        return md5Key;
    }

    public int writeTo(ByteBuffer buffer) throws IOException {
        this.buffer.position(0);
        buffer.putInt(length()).put(this.buffer);
        return LENGTH_BYTES + length();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Item item = (Item) o;

        if (buffer != null ? !buffer.equals(item.buffer) : item.buffer != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return buffer != null ? buffer.hashCode() : 0;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Item");
        sb.append("{buffer=").append(buffer);
        sb.append('}');
        return sb.toString();
    }

    public void writeTo(RandomAccessFile randomAccessFile) {
        throw new UnsupportedOperationException();
    }

    public static Item readFrom(RandomAccessFile randomAccessFile) {
        throw new UnsupportedOperationException();
    }
}
