package com.github.zhongl.store;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * {@link Item} is wrapper of bytes, it provids read and write operation.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public class Item {
    private final byte[] bytes;

    public static Item readFrom(DataInput dataInput, int length) throws IOException {
        byte[] bytes = new byte[length];
        dataInput.readFully(bytes);
        return new Item(bytes);
    }

    public Item(byte[] bytes) {
        this.bytes = bytes;
    }

    public int byteLength() {
        return bytes.length;
    }

    public void writeTo(DataOutput output) throws IOException {
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
