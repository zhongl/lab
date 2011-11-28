package com.taobao.common.store.journal.impl;

import com.google.common.base.Preconditions;
import com.taobao.common.store.journal.OpItem;
import com.taobao.common.store.util.BytesKey;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;


/**
 * 基于开放地址法，存储于硬盘上的HashMap
 *
 * @author boyan
 * @since 1.0, 2009-10-20 上午11:27:07
 */

public class OpItemHashMap {

    public static final int DEFAULT_CAPACITY = 256;

    private final OpItemEntry[] table;

    private final BitSet bitSet;

    private final File file;

    private final RandomAccessFile randomAccessFile;

    private final MappedByteBuffer mappedByteBuffer;


    public OpItemHashMap(int capacity, String cacheFilePath, boolean force) throws IOException {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity<=0");
        }
        this.file = createOrOverwriteFile(cacheFilePath);
        this.bitSet = new BitSet(OpItemEntry.SIZE * capacity);
        this.randomAccessFile = new RandomAccessFile(file, force ? "rws" : "rw");
        this.mappedByteBuffer =
                randomAccessFile.getChannel().map(MapMode.READ_WRITE, OpItemEntry.SIZE * capacity / 2, OpItemEntry.SIZE * capacity);
        this.table = new OpItemEntry[capacity];
    }

    private File createOrOverwriteFile(String cacheFilePath) {
        File file = new File(cacheFilePath);
        if (file.exists()) {
            Preconditions.checkArgument(file.isFile(), "%s should be a file.", cacheFilePath);
            Preconditions.checkArgument(file.delete(), "%s can't overwrite.", cacheFilePath);
        }
        file.getParentFile().mkdirs();
        return file;
    }


    private int rehash(BytesKey key, int i) {
        return abs(hash(key) + i * key.hashCode() % (table.length - 2)) % table.length; // 双重散列
    }


    private int hash(BytesKey key) {
        return abs(key.hashCode()) % table.length;
    }


//    private int hashForKey(BytesKey k) {
//        int hash = k.hashCode();
//        return abs(hash);
//    }


    private int abs(int hash) {
        if (hash == Integer.MIN_VALUE) {
            hash = 0;
        }
        return Math.abs(hash);
    }


    public boolean put(BytesKey key, OpItem value) throws IOException {
        if (this.loadFactor() > 0.75f) {
            return false;
        }
        int j = hash(key);
        int offset = calcOffset(j);
        int i = 0;
        // 定位
        while (this.table[j] != null && !isEntryDeleted(j) && this.bitSet.get(offset) && i < table.length) {
            j = rehash(key, i++);
            offset = calcOffset(j);
        }

        if (table[j] == null || table[j].isDeleted()) {
            table[j] = new OpItemEntry(value, false);
            byte[] buffer = table[j].encode();
            if (buffer != null) {
                write(offset, buffer);
                bitSet.set(offset, true);
            }
            // 从内存释放
            table[j].unload();
            return true;
        } else {
            return false;
        }

    }

    private void write(int offset, byte[] buffer) {
        this.mappedByteBuffer.position(offset);
        this.mappedByteBuffer.put(buffer, 0, buffer.length);
    }

    private int calcOffset(int j) {
        return j * OpItemEntry.SIZE;
    }


    private boolean isEntryDeleted(int j) throws IOException {
        if (!this.table[j].isLoaded()) {
            this.table[j].load(mappedByteBuffer, calcOffset(j), false);
        }
        this.table[j].unload(); // 记得释放
        return this.table[j].isDeleted();
    }

    public OpItem get(BytesKey key) throws IOException {
        int j = hash(key);
        int i = 0;
        int m = table.length;
        while (this.table[j] != null && i < m) {
            if (!table[j].isLoaded()) {
                table[j].load(this.mappedByteBuffer, calcOffset(j), true);
            }
            if (table[j].getOpItem() != null && Arrays.equals(table[j].getOpItem().getKey(), key.getData())) {
                if (table[j].isDeleted()) {
                    return null;
                } else {
                    return table[j].getOpItem();
                }
            } else {
                table[j].unload();// 记住清除
            }
            j = rehash(key, i++);
        }
        return null;
    }


    public OpItem remove(BytesKey key) throws IOException {

        int j = hash(key);
        int i = 0;
        int m = table.length;
        while (this.table[j] != null && i < m) {
            int offset = calcOffset(j);
            if (!table[j].isLoaded()) {
                table[j].load(mappedByteBuffer, offset, true);
            }
            if (table[j].getOpItem() != null && Arrays.equals(table[j].getOpItem().getKey(), key.getData())) {
                if (table[j].isDeleted()) {
                    return null;
                } else {
                    table[j].setDeleted(true);
                    this.bitSet.set(offset, false);
                    // 写入磁盘
                    this.mappedByteBuffer.put(offset, DELETED);
                    return table[j].getOpItem();
                }
            } else {
                table[j].unload();// 切记unload
            }
            j = rehash(key, i++);
        }
        return null;

    }


    public void close() throws IOException {
        randomAccessFile.close();
        file.delete();
    }

    class DiskIterator implements java.util.Iterator<BytesKey> {
        private int currentIndex = 0;
        private int lastRet = -1;


        public boolean hasNext() {

            int i = this.currentIndex;
            if (i >= table.length) {
                return false;
            }
            while (!isExists(i)) {
                if (i == table.length - 1) {
                    return false;
                }
                i++;
            }
            return true;

        }


        private boolean isExists(int i) {
            return table[i] != null && !table[i].isDeleted();
        }


        public BytesKey next() {
            try {
                if (currentIndex >= table.length) {
                    return null;
                }
                while (!isExists(currentIndex)) {
                    if (currentIndex == table.length - 1) {
                        return null;
                    }
                    currentIndex++;
                }
                if (!table[currentIndex].isLoaded()) {
                    table[currentIndex].load(mappedByteBuffer, calcOffset(currentIndex), true);
                }
                BytesKey key = new BytesKey(table[currentIndex].getOpItem().getKey());
                this.currentIndex++;
                this.lastRet++;
                return key;
            } catch (IOException e) {
                throw new IllegalStateException("Load OpItem fail", e);
            }

        }


        public void remove() {
            if (this.lastRet == -1) {
                throw new IllegalStateException("The next method is not been called");
            }
            table[currentIndex - 1].setDeleted(true);
            bitSet.set(calcOffset(currentIndex - 1), false);
            // 写入磁盘
            mappedByteBuffer.put(calcOffset(currentIndex - 1), DELETED);
            lastRet = -1;
        }

    }

    public Iterator<BytesKey> iterator() {
        return new DiskIterator();
    }

    static final byte DELETED = (byte) 1;


    public int size() {
        return this.bitSet.cardinality();
    }


    private float loadFactor() {
        return (float) this.bitSet.cardinality() / this.table.length;
    }

}
