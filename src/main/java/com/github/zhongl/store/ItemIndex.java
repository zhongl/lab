package com.github.zhongl.store;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class ItemIndex {
    public static final int BYTES = 8/* offset takes 8 bytes */ + 4/* length takes 4 bytes */;

    private final long offset;
    private final int length;

    public static ItemIndex readFrom(DataInput dataInput) throws IOException {
        return new ItemIndex(dataInput.readLong(), dataInput.readInt());
    }

    public ItemIndex(long offset, int length) {
        this.offset = offset;
        this.length = length;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("offset", offset)
                .add("length", length).toString();
    }

    public long offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    public void writeTo(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(offset);
        dataOutput.writeInt(length);
    }
}
