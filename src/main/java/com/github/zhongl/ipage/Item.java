package com.github.zhongl.ipage;

import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * {@link Item} is wrapper of bytes, it provids read and write operation.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@ThreadSafe
public final class Item {
    public static final long LENGTH_BYTES = 4;
    private final byte[] bytes;
    private final Md5Key md5Key;

    public static Item readFrom(DataInput dataInput) throws IOException {
        int length = dataInput.readInt();
        byte[] bytes = new byte[length];
        dataInput.readFully(bytes);
        return new Item(bytes);
    }

    public Item(byte[] bytes) {
        this.bytes = bytes;
        md5Key = Md5Key.valueOf(this.bytes);
    }

    public int length() {
        return bytes.length;
    }

    public Md5Key md5Key() {
        return md5Key;
    }

    public void writeTo(DataOutput output) throws IOException {
        output.writeInt(length());
        output.write(bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("bytes", Arrays.toString(bytes))
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Item)) return false;
        Item that = (Item) obj;
        return Arrays.equals(this.bytes, that.bytes);
    }
}
