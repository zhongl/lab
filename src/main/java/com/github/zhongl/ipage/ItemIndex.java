package com.github.zhongl.ipage;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public final class ItemIndex {
    public static final int LENGTH = 4 + 8;
    private final int pageIndex;
    private final long offset;

    public ItemIndex(int pageIndex, long offset) {
        this.pageIndex = pageIndex;
        this.offset = offset;
    }

    public int pageIndex() {
        return pageIndex;
    }

    public long offset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ItemIndex itemIndex = (ItemIndex) o;

        if (offset != itemIndex.offset) return false;
        if (pageIndex != itemIndex.pageIndex) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = pageIndex;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ItemIndex");
        sb.append("{pageIndex=").append(pageIndex);
        sb.append(", offset=").append(offset);
        sb.append('}');
        return sb.toString();
    }

    public static ItemIndex readFrom(ByteBuffer buffer) {
        int pageIndex = buffer.getInt();
        long offset = buffer.getLong();
        return new ItemIndex(pageIndex, offset);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(pageIndex);
        buffer.putLong(offset);
    }
}
