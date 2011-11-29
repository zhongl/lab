package com.github.zhongl.ipage;

import com.google.common.io.Files;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * {@link com.github.zhongl.ipage.RecordIndex} is a dir-based hash map for mapping
 * {@link com.github.zhongl.ipage.Md5Key} and offset of {@link com.github.zhongl.ipage.Record} in
 * {@link com.github.zhongl.ipage.IPage}.
 * <p/>
 * It is a implemente of separate chain hash table, more infomation you can find in "Data Structures & Algorithms In Java".
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public final class RecordIndex implements Closeable {

    private static final Long NULL_OFFSET = null;
    private final MappedByteBuffer mappedByteBuffer;
    private final Bucket[] buckets;

    public RecordIndex(File file, int initCapacity) throws IOException {
        mappedByteBuffer = Files.map(file, READ_WRITE, initCapacity);
        buckets = createBuckets(initCapacity / Bucket.LENGTH);
    }

    public Long put(Md5Key key, Long offset) {
        return buckets[hashAndMode(key)].put(key, offset);
    }

    public Long get(Md5Key key) {
        return buckets[hashAndMode(key)].get(key);
    }

    public Long remove(Md5Key key) {
        return buckets[hashAndMode(key)].remove(key);
    }

    @Override
    public void close() throws IOException {
        flush();
        DirectByteBufferCleaner.clean(mappedByteBuffer);
    }

    private int hashAndMode(Md5Key key) {
        return Math.abs(key.hashCode()) % buckets.length;
    }

    private Bucket[] createBuckets(int size) {
        Bucket[] buckets = new Bucket[size];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new Bucket(slice(mappedByteBuffer, i * Bucket.LENGTH, Bucket.LENGTH));
        }
        return buckets;
    }

    public void flush() {
        mappedByteBuffer.force();
    }

    private static ByteBuffer slice(ByteBuffer buffer, int offset, int length) {
        buffer.position(offset);
        buffer.limit(offset + length);
        return buffer.slice();
    }

    /**
     * {@link com.github.zhongl.ipage.RecordIndex.Bucket} has 163
     * {@link com.github.zhongl.ipage.RecordIndex.Bucket.Slot} for storing tuple of
     * {@link com.github.zhongl.ipage.Md5Key} and offset of {@link com.github.zhongl.ipage.Record} in
     * {@link com.github.zhongl.ipage.IPage}.
     * <p/>
     * Every slot has a head byte to indicate it is empty, occupied or released.
     */
    private static class Bucket {

        public static final int LENGTH = 4096; // 4K
        private final Slot[] slots;

        public Bucket(ByteBuffer buffer) {
            slots = createSlots(buffer);
        }

        private Slot[] createSlots(ByteBuffer buffer) {
            Slot[] slots = new Slot[LENGTH / Slot.LENGTH];
            for (int i = 0; i < slots.length; i++) {
                slots[i] = new Slot(slice(buffer, i * Slot.LENGTH, Slot.LENGTH));
            }
            return slots;
        }

        public Long put(Md5Key key, Long offset) {
            int firstReleased = -1;
            for (int i = 0; i < slots.length; i++) {
                switch (slots[i].state()) {
                    case EMPTY:
                        return slots[i].add(key, offset);
                    case OCCUPIED:
                        if (slots[i].keyEquals(key)) return slots[i].replace(key, offset);
                        break;
                    case RELEASED:
                        if (firstReleased < 0) firstReleased = i;
                        break;
                }
                // continue to check rest slots whether contain the key.
            }
            if (firstReleased < 0) throw new OverflowException("No slot for new item index.");
            Long previous = slots[firstReleased].add(key, offset);
            updateDigest();
            return previous;
        }

        public Long get(Md5Key key) {
            for (Slot slot : slots) {
                if (slot.state() == Slot.State.EMPTY) return NULL_OFFSET; // because rest slots are all empty
                if (slot.state() == Slot.State.OCCUPIED && slot.keyEquals(key)) return slot.offset();
            }
            return NULL_OFFSET;
        }

        public Long remove(Md5Key key) {
            for (Slot slot : slots) {
                if (slot.state() == Slot.State.EMPTY) return NULL_OFFSET; // because rest slots are all empty
                if (slot.state() == Slot.State.OCCUPIED && slot.keyEquals(key)) {
                    Long previous = slot.release();
                    updateDigest();
                    return previous;
                }
            }
            return NULL_OFFSET;
        }

        private void updateDigest() {
            // TODO updateDigest
        }

        /**
         * {@link com.github.zhongl.ipage.RecordIndex.Bucket.Slot} =
         * {@link com.github.zhongl.ipage.RecordIndex.Bucket.Slot.State}
         * {@link com.github.zhongl.ipage.Md5Key}
         * {@link Long}
         */
        private static class Slot {

            public static final int LENGTH = 1/*head byte*/ + Md5Key.LENGTH + 8 /*offset:Long*/;
            private final ByteBuffer buffer;

            public Slot(ByteBuffer buffer) {
                this.buffer = buffer;
            }

            public Long add(Md5Key key, Long offset) {
                buffer.position(0);
                buffer.put(State.OCCUPIED.toByte());
                key.writeTo(buffer);
                buffer.putLong(offset);
                return null;
            }

            public State state() {
                return State.valueOf(buffer.get(0));
            }

            public boolean keyEquals(Md5Key key) {
                buffer.position(1); // skip head byte
                return Md5Key.readFrom(buffer).equals(key);
            }

            public Long replace(Md5Key key, Long offset) {
                Long previous = offset();
                add(key, offset);
                return previous;
            }

            public Long offset() {
                buffer.position(1 + Md5Key.LENGTH); // skip head byte and key
                return buffer.getLong();
            }

            public Long release() {
                buffer.put(0, State.RELEASED.toByte());
                return offset();
            }

            enum State {
                EMPTY, OCCUPIED, RELEASED;

                public byte toByte() {
                    return (byte) this.ordinal();
                }

                public static State valueOf(byte b) {
                    if (EMPTY.toByte() == b) return EMPTY;
                    if (OCCUPIED.toByte() == b) return OCCUPIED;
                    if (RELEASED.toByte() == b) return RELEASED;
                    throw new IllegalStateException("Unknown slot state: " + b);
                }
            }
        }

    }
}
