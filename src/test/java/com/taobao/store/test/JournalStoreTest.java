/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package com.taobao.store.test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.common.store.journal.JournalStore;
import com.taobao.common.store.journal.OpItem;
import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.UniqId;


/**
 * @author dogun (yuexuqiang at gmail.com)
 * @author lin wang(xalinx at gmail dot com)
 * @date 2007-12-10
 */
public class JournalStoreTest {
    JournalStore store = null;


    private String getPath() {
        return "tmp" + File.separator + "notify-store-test";
    }


    private String getStoreName() {
        return "testStore";
    }


    @Before
    public void setUp() throws Exception {
        String path = this.getPath();
        File dir = new File(path);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("can't make dir " + dir);
        }

        File[] fs = dir.listFiles();
        for (File f : fs) {
            if (!f.delete()) {
                throw new IllegalStateException("can't delete " + f);
            }
        }

        this.store = new JournalStore(path, this.getStoreName());
        assertEquals(0, this.store.size());
    }


    @After
    public void after() throws IOException {
        if (this.store != null) {
            this.store.close();
        }
    }


    /**
     * * @throws Exception
     */
    @Test
    public void testDeletingUnusedNonCurrentFileWhileCreatingStore() throws Exception {
        this.after();
        String filePrefix = this.getFilePrefix();

        RandomAccessFile df = new RandomAccessFile(filePrefix + "1", "rw");
        RandomAccessFile lf = new RandomAccessFile(filePrefix + "1.log", "rw");
        final int messageLength = 1024 * 1024 * 8;
        while (df.length() < JournalStore.FILE_SIZE) {
            byte[] key = UniqId.getInstance().getUniqIDHash();
            long offset = this.add(df, lf, key, new byte[messageLength], 1);
            this.remove(lf, key, offset, 1, messageLength);
        }
        df.close();
        lf.close();

        df = new RandomAccessFile(filePrefix + "2", "rw");
        lf = new RandomAccessFile(filePrefix + "2.log", "rw");

        df.close();
        lf.close();
        this.store = new JournalStore(this.getPath(), this.getStoreName());

        Assert.assertFalse(new File(filePrefix + "1").exists());
        Assert.assertFalse(new File(filePrefix + "1.log").exists());

    }


    private String getFilePrefix() {
        return this.getPath() + File.separator + this.getStoreName() + ".";
    }


    @Test
    public void testAdd_UpdateInSameFile1() throws Exception {
        byte[] key = UniqId.getInstance().getUniqIDHash();
        this.store.add(key, "OriginalData".getBytes());
        this.store.update(key, "FirstUpdate".getBytes());
        this.store.update(key, "LastUpdate".getBytes());

        assertEquals(0, "LastUpdate".compareTo(new String(this.store.get(key))));

        final int messageLength = 1024 * 1024 * 8;
        this.store.remove(key);

        final int count = JournalStore.FILE_SIZE / messageLength + 3;

        for (int i = 0; i < count; i++) {
            byte[] data = new byte[messageLength];
            byte[] k = UniqId.getInstance().getUniqIDHash();
            this.store.add(k, data);
            this.store.remove(k);
        }

        String filePrefix = this.getFilePrefix();
        Assert.assertFalse(new File(filePrefix + "1").exists());
        Assert.assertFalse(new File(filePrefix + "1.log").exists());
    }


    @Test
    public void testAdd_UpdateInSameFile2() throws Exception {
        byte[] key = UniqId.getInstance().getUniqIDHash();
        this.store.add(key, "OriginalData".getBytes());
        this.store.update(key, "FirstUpdate".getBytes());
        this.store.update(key, "LastUpdate".getBytes());

        this.after();

        this.store = new JournalStore(this.getPath(), this.getStoreName());
        assertEquals(0, "LastUpdate".compareTo(new String(this.store.get(key))));

        final int messageLength = 1024 * 1024 * 8;
        this.store.remove(key);

        final int count = JournalStore.FILE_SIZE / messageLength + 3;

        for (int i = 0; i < count; i++) {
            byte[] data = new byte[messageLength];
            byte[] k = UniqId.getInstance().getUniqIDHash();
            this.store.add(k, data);
            this.store.remove(k);
        }

        String filePrefix = this.getFilePrefix();
        Assert.assertFalse(new File(filePrefix + "1").exists());
        Assert.assertFalse(new File(filePrefix + "1.log").exists());
    }


    @Test
    public void testAdd_UpdateInSameFile3() throws Exception {
        byte[] key = UniqId.getInstance().getUniqIDHash();
        this.store.add(key, "OriginalData".getBytes());
        this.store.update(key, "LastUpdate".getBytes());

        assertEquals(0, "LastUpdate".compareTo(new String(this.store.get(key))));

        final int messageLength = 1024 * 1024 * 8;
        this.store.remove(key);

        final int count = JournalStore.FILE_SIZE / messageLength + 3;

        for (int i = 0; i < count; i++) {
            byte[] data = new byte[messageLength];
            byte[] k = UniqId.getInstance().getUniqIDHash();
            this.store.add(k, data);
            this.store.remove(k);
        }

        String filePrefix = this.getFilePrefix();
        Assert.assertFalse(new File(filePrefix + "1").exists());
        Assert.assertFalse(new File(filePrefix + "1.log").exists());
    }


    @Test
    public void testAdd_UpdateInSameFile4() throws Exception {
        byte[] key = UniqId.getInstance().getUniqIDHash();
        this.store.add(key, "OriginalData".getBytes());
        this.store.update(key, "LastUpdate".getBytes());

        this.after();

        this.store = new JournalStore(this.getPath(), this.getStoreName());
        assertEquals(0, "LastUpdate".compareTo(new String(this.store.get(key))));

        final int messageLength = 1024 * 1024 * 8;
        this.store.remove(key);

        final int count = JournalStore.FILE_SIZE / messageLength + 3;

        for (int i = 0; i < count; i++) {
            byte[] data = new byte[messageLength];
            byte[] k = UniqId.getInstance().getUniqIDHash();
            this.store.add(k, data);
            this.store.remove(k);
        }

        String filePrefix = this.getFilePrefix();
        Assert.assertFalse(new File(filePrefix + "1").exists());
        Assert.assertFalse(new File(filePrefix + "1.log").exists());
    }


    @Test
    public void testAdd_UpdateInDifferentFile1() throws Exception {
        byte[] key = UniqId.getInstance().getUniqIDHash();
        byte[] key2 = UniqId.getInstance().getUniqIDHash();
        this.store.add(key, "OriginalData".getBytes());
        this.store.add(key2, "SecondData".getBytes());

        final int messageLength = 1024 * 1024 * 8;
        final int count = JournalStore.FILE_SIZE / messageLength + 3;

        for (int i = 0; i < count; i++) {
            byte[] data = new byte[messageLength];
            byte[] k = UniqId.getInstance().getUniqIDHash();
            this.store.add(k, data);
            this.store.remove(k);
        }

        this.store.update(key, "FirstUpdate".getBytes());

        this.store.sync();
        Thread.sleep(100);
        
        RandomAccessFile f = new RandomAccessFile(this.getFilePrefix() + "1.log", "r");
        f.seek(f.length() - OpItem.LENGTH);
        byte[] opItem = new byte[OpItem.LENGTH];
        f.read(opItem);
        f.close();

        byte[] keyRead = copyOf(opItem, 16);
        Assert.assertTrue(new BytesKey(key).equals(new BytesKey(keyRead)));
        assertEquals(OpItem.OP_DEL, opItem[16]);
        assertEquals(0, opItem[17]);
        assertEquals(0, opItem[18]);
        assertEquals(0, opItem[19]);
        assertEquals(1, opItem[20]);

        this.store.remove(key2);

        this.store.sync();
        Thread.sleep(500);
        Assert.assertFalse(new File(this.getFilePrefix() + "1").exists());
        Assert.assertFalse(new File(this.getFilePrefix() + "1.log").exists());

        this.store.update(key, "LastUpdate".getBytes());
        assertEquals(0, "LastUpdate".compareTo(new String(this.store.get(key))));

    }


    public static byte[] copyOf(byte[] original, int newLength) {
        byte[] copy = new byte[newLength];
        System.arraycopy(original, 0, copy, 0, Math.min(original.length, newLength));
        return copy;
    }


    @Test
    public void testAdd_UpdateInDifferentFile2() throws Exception {
        byte[] key = UniqId.getInstance().getUniqIDHash();
        byte[] key2 = UniqId.getInstance().getUniqIDHash();
        this.store.add(key, "OriginalData".getBytes());
        this.store.add(key2, "SecondData".getBytes());
        this.store.update(key, "FirstUpdate".getBytes());
        this.store.update(key, "SecondUpdate".getBytes());

        final int messageLength = 1024 * 1024 * 8;
        final int count = JournalStore.FILE_SIZE / messageLength + 3;

        for (int i = 0; i < count; i++) {
            byte[] data = new byte[messageLength];
            byte[] k = UniqId.getInstance().getUniqIDHash();
            this.store.add(k, data);
            this.store.remove(k);
        }

        this.store.update(key, "FirstUpdate".getBytes());

        this.store.sync();
        Thread.sleep(500);
        
        RandomAccessFile f = new RandomAccessFile(this.getFilePrefix() + "1.log", "r");
        f.seek(f.length() - OpItem.LENGTH);
        byte[] opItem = new byte[OpItem.LENGTH];
        f.read(opItem);
        f.close();

        byte[] keyRead = copyOf(opItem, 16);
        Assert.assertTrue(new BytesKey(key).equals(new BytesKey(keyRead)));
        assertEquals(OpItem.OP_DEL, opItem[16]);
        assertEquals(0, opItem[17]);
        assertEquals(0, opItem[18]);
        assertEquals(0, opItem[19]);
        assertEquals(1, opItem[20]);

        this.store.remove(key2);

        this.store.sync();
        Thread.sleep(500);
        
        Assert.assertFalse(new File(this.getFilePrefix() + "1").exists());
        Assert.assertFalse(new File(this.getFilePrefix() + "1.log").exists());

        this.store.update(key, "LastUpdate".getBytes());
        assertEquals(0, "LastUpdate".compareTo(new String(this.store.get(key))));

    }


    @Test
    public void testBrokenUpdate() throws Exception {
        byte[] key = UniqId.getInstance().getUniqIDHash();
        byte[] key2 = UniqId.getInstance().getUniqIDHash();

        this.store.add(key, "OriginalData".getBytes());
        this.store.add(key2, "SecondData".getBytes());

        final int messageLength = 1024 * 1024 * 8;
        final int count = JournalStore.FILE_SIZE / messageLength + 3;

        for (int i = 0; i < count; i++) {
            byte[] data = new byte[messageLength];
            byte[] k = UniqId.getInstance().getUniqIDHash();
            this.store.add(k, data);
            this.store.remove(k);
        }

        Assert.assertTrue(new File(this.getFilePrefix() + "1").exists());
        Assert.assertTrue(new File(this.getFilePrefix() + "1.log").exists());
        Assert.assertTrue(new File(this.getFilePrefix() + "2").exists());
        Assert.assertTrue(new File(this.getFilePrefix() + "2.log").exists());

        this.after();

        RandomAccessFile df = new RandomAccessFile(this.getFilePrefix() + "2", "rw");
        RandomAccessFile lf = new RandomAccessFile(this.getFilePrefix() + "2.log", "rw");
        this.add(df, lf, key, "FirstUpdate".getBytes(), 2);

        df.close();
        lf.close();

        this.store = new JournalStore(this.getPath(), this.getStoreName());

        RandomAccessFile f = new RandomAccessFile(this.getFilePrefix() + "1.log", "r");
        f.seek(f.length() - OpItem.LENGTH);
        byte[] opItem = new byte[OpItem.LENGTH];
        f.read(opItem);
        f.close();

        this.store.remove(key2);
        this.store.sync();
        Thread.sleep(500);
        
        Assert.assertFalse(new File(this.getFilePrefix() + "1").exists());
        Assert.assertFalse(new File(this.getFilePrefix() + "1.log").exists());

        for (int i = 0; i < count; i++) {
            byte[] data = new byte[messageLength];
            byte[] k = UniqId.getInstance().getUniqIDHash();
            this.store.add(k, data);
            this.store.remove(k);
        }

        this.store.remove(key);
        Assert.assertFalse(new File(this.getFilePrefix() + "2").exists());
        Assert.assertFalse(new File(this.getFilePrefix() + "2.log").exists());
    }


    @Test
    public void testBrokenIndex() throws Exception {
        this.after();
        RandomAccessFile df = new RandomAccessFile(this.getFilePrefix() + "1", "rw");
        RandomAccessFile lf = new RandomAccessFile(this.getFilePrefix() + "1.log", "rw");

        byte[] key = UniqId.getInstance().getUniqIDHash();
        byte[] key2 = UniqId.getInstance().getUniqIDHash();
        this.add(df, lf, key, "Message".getBytes(), 1);
        lf.write(key2);

        df.close();
        lf.close();

        this.store = new JournalStore(this.getPath(), this.getStoreName());
        assertEquals(0, "Message".compareTo(new String(this.store.get(key))));
        this.after();

        lf = new RandomAccessFile(this.getFilePrefix() + "1.log", "r");
        assertEquals(OpItem.LENGTH, lf.length());
        lf.close();
    }


    private void remove(RandomAccessFile lf, byte[] key, long offset, int number, int dataLength) throws IOException {
        lf.write(key);
        lf.write(OpItem.OP_DEL);
        lf.writeInt(number);
        lf.writeLong(offset);
        lf.writeInt(dataLength);
    }


    private long add(RandomAccessFile df, RandomAccessFile lf, byte[] key, byte[] data, int number) throws IOException {
        df.write(data);
        lf.write(key);
        lf.write(OpItem.OP_ADD);
        lf.writeInt(number);
        lf.writeLong(df.length() - data.length);
        lf.writeInt(data.length);
        return df.length() - data.length;
    }


    @Test
    public void testAddGetRemoveMixed() throws Exception {
        long s = System.currentTimeMillis();
        byte[] key = UniqId.getInstance().getUniqIDHash();
        for (int k = 0; k < 10000; ++k) {
            this.store.add(key, "hellofdfdfdfdfd".getBytes());
            byte[] data = this.store.get(key);
            assertNotNull(data);
            assertEquals("hellofdfdfdfdfd", new String(data));
            assertEquals(1, this.store.size());
            this.store.remove(key);
            assertEquals(0, this.store.size());
            data = this.store.get(key);
            assertNull(data);
            assertEquals(0, this.store.size());
        }
        System.out.println(System.currentTimeMillis() - s + "ms");
    }


    @Test
    public void testLoadAddReadRemove10K() throws Exception {
        this.loadAddReadRemove(this.getMsg10K());
    }


    @Test
    public void testLoadAddReadRemove1K() throws Exception {
        this.loadAddReadRemove(this.getMsg1K());
    }


    public void loadAddReadRemove(String msg) throws Exception {
        int num = 100000;
        // load add
        long s = System.currentTimeMillis();
        for (int k = 0; k < num; k++) {
            this.store.add(getId(k, k), msg.getBytes());
        }
        s = System.currentTimeMillis() - s;
        System.out.println("add " + msg.getBytes().length + " bytes " + num + " times waste " + s + "ms, average " + s
                * 1.0d / num);
        assertEquals(num, this.store.size());

        // load read
        s = System.currentTimeMillis();
        for (int k = 0; k < num; k++) {
            this.store.get(getId(k, k));
        }
        s = System.currentTimeMillis() - s;
        System.out.println("get " + msg.getBytes().length + " bytes " + num + " times waste " + s + "ms, average " + s
                * 1.0d / num);

        // load remove
        s = System.currentTimeMillis();
        for (int k = 0; k < num; k++) {
            this.store.remove(getId(k, k));
        }
        s = System.currentTimeMillis() - s;
        System.out.println("remove " + num + " times waste " + s + "ms, average " + s * 1.0d / num);
        assertEquals(0, this.store.size());
    }


    
    public void testLoadHeavy() throws Exception {
        this.load(8, 2000, 5);
    }



    public void testLoadMin() throws Exception {
        this.load(2, 2000, 5);
    }


    public void load(int ThreadNum, int totalPerThread, long meantime) throws Exception {
        MsgCreator[] mcs = new MsgCreator[ThreadNum];
        MsgRemover[] mrs = new MsgRemover[mcs.length];
        for (int i = 0; i < mcs.length; i++) {
            MsgCreator mc = new MsgCreator(i, totalPerThread, meantime);
            mcs[i] = mc;
            mc.start();
            MsgRemover mr = new MsgRemover(i, totalPerThread, meantime);
            mrs[i] = mr;
            mr.start();
        }

        for (int i = 0; i < mcs.length; i++) {
            mcs[i].join();
            mrs[i].join();
        }

        assertEquals(0, this.store.size());

        long totalAddTime = 0;
        long totalRemoveTime = 0;
        for (int i = 0; i < mcs.length; i++) {
            totalAddTime += mcs[i].timeTotal;
            totalRemoveTime += mrs[i].timeTotal;
        }

        System.out.println(totalPerThread * ThreadNum * 2 + " of " + ThreadNum * 2 + " thread average: add "
                + totalAddTime * 1.0d / (totalPerThread * ThreadNum) + ", remove " + totalRemoveTime * 1.0d
                / (totalPerThread * ThreadNum));
    }


    static byte[] getId(int id, int seq) {
        final byte tmp[] = new byte[16];
        tmp[0] = (byte) ((0xff000000 & id) >> 24);
        tmp[1] = (byte) ((0xff0000 & id) >> 16);
        tmp[2] = (byte) ((0xff00 & id) >> 8);
        tmp[3] = (byte) (0xff & id);

        tmp[4] = (byte) ((0xff000000 & seq) >> 24);
        tmp[5] = (byte) ((0xff0000 & seq) >> 16);
        tmp[6] = (byte) ((0xff00 & seq) >> 8);
        tmp[7] = (byte) (0xff & seq);
        return tmp;
    }

    private static final byte[] MSG_BYTES = new byte[102400];


    private String getMsg1K() {
        return new String(MSG_BYTES, 0, 1024);
    }


    private String getMsg10K() {
        return new String(MSG_BYTES, 0, 10240);
    }

    private class MsgCreator extends Thread {
        int id;

        int totalPerThread;

        long timeTotal;

        long meantime;


        MsgCreator(int id, int totalPerThread, long meantime) {
            this.id = id;
            this.totalPerThread = totalPerThread;
            this.meantime = meantime;
        }


        @Override
        public void run() {
            for (int k = 0; k < this.totalPerThread; k++) {
                try {
                    Thread.sleep(this.meantime);
                    long start = System.currentTimeMillis();
                    JournalStoreTest.this.store.add(JournalStoreTest.getId(this.id, k), JournalStoreTest.this.getMsg1K().getBytes());
                    this.timeTotal += System.currentTimeMillis() - start;
                }
                catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private class MsgRemover extends Thread {
        int id;

        int totalPerThread;

        long timeTotal;

        long meantime;


        MsgRemover(int id, int totalPerThread, long meantime) {
            this.id = id;
            this.totalPerThread = totalPerThread;
            this.meantime = meantime;
        }


        @Override
        public void run() {
            for (int k = 0; k < this.totalPerThread;) {
                try {
                    Thread.sleep(this.meantime);
                    byte[] read = JournalStoreTest.this.store.get(JournalStoreTest.getId(this.id, k));
                    if (read == null) {
                        continue;
                    }
                    long start = System.currentTimeMillis();
                    boolean success = JournalStoreTest.this.store.remove(JournalStoreTest.getId(this.id, k));
                    this.timeTotal += System.currentTimeMillis() - start;
                    if (!success) {
                        throw new IllegalStateException();
                    }
                    k++;
                }
                catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }
}
