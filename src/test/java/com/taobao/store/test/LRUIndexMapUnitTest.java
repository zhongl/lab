package com.taobao.store.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.common.store.journal.OpItem;
import com.taobao.common.store.journal.impl.LRUIndexMap;
import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.UniqId;


/**
 * 
 * 
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-10-22 下午12:46:40
 */

public class LRUIndexMapUnitTest {
    private LRUIndexMap map;


    private String getPath() {
        return "tmp" + File.separator + "LRUIndexMapUnitTest";
    }


    private String getCacheFileName() {
        return "testIndex";
    }


    @Before
    public void setUp() {
        String path = getPath();
        File dir = new File(path);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("can't make dir " + dir);
        }

        // File[] fs = dir.listFiles();
        // for (File f : fs) {
        // if (!f.delete()) {
        // throw new IllegalStateException("can't delete " + f);
        // }
        // }
    }


    @Test
    public void testCrud() throws Exception {
        map = new LRUIndexMap(1000, getPath() + File.separator + getCacheFileName(), false);
        Assert.assertEquals(0, map.size());
        for (int i = 0; i < 100; i++) {
            final BytesKey key = new BytesKey(UniqId.getInstance().getUniqIDHash());
            OpItem opItem = new OpItem();
            opItem.setKey(key.getData());
            opItem.setLength(2);
            opItem.setNumber(10);
            opItem.setOffset(10000);
            opItem.setOp((byte) 0);
            Assert.assertNull(this.map.get(key));
            this.map.put(key, opItem);
            Assert.assertEquals(opItem, this.map.get(key));
            map.remove(key);
            Assert.assertNull(this.map.get(key));
        }
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(0, map.getMap().size());
    }


    @Test
    public void testLRU() throws Exception {
        map = new LRUIndexMap(10, getPath() + File.separator + getCacheFileName(), true);
        for (int i = 0; i < 10; i++) {
            final BytesKey key = new BytesKey(UniqId.getInstance().getUniqIDHash());
            OpItem opItem = new OpItem();
            opItem.setKey(key.getData());
            opItem.setLength(2);
            opItem.setNumber(10);
            opItem.setOffset(10000);
            opItem.setOp((byte) 0);
            this.map.put(key, opItem);
        }
        Assert.assertEquals(10, map.size());
        Assert.assertEquals(10, map.getMap().size());

        // 替换
        BytesKey key = new BytesKey(UniqId.getInstance().getUniqIDHash());
        OpItem opItem = new OpItem();
        opItem.setKey(key.getData());
        opItem.setLength(2);
        opItem.setNumber(10);
        opItem.setOffset(10000);
        opItem.setOp((byte) 0);
        this.map.put(key, opItem);

        Assert.assertEquals(11, map.size());
        Assert.assertEquals(10, map.getMap().size());
        Assert.assertEquals(1, map.getHandler().getDiskMap().size());

        for (int i = 0; i < 10; i++) {
            key = new BytesKey(UniqId.getInstance().getUniqIDHash());
            opItem = new OpItem();
            opItem.setKey(key.getData());
            opItem.setLength(2);
            opItem.setNumber(10);
            opItem.setOffset(10000);
            opItem.setOp((byte) 0);
            this.map.put(key, opItem);
        }

        Assert.assertEquals(21, map.size());
        Assert.assertTrue(map.getHandler().getDiskMap().size() > 0);
        Assert.assertEquals(21, map.getMap().size() + map.getHandler().getDiskMap().size());

    }


    @Test
    public void testPutAll() throws Exception {
        map = new LRUIndexMap(5000, getPath() + File.separator + getCacheFileName(), true);

        Map<BytesKey, OpItem> tmpMap = new HashMap<BytesKey, OpItem>();
        // 插入1万数据，遍历
        for (int i = 0; i < 1000; i++) {
            BytesKey key = new BytesKey(UniqId.getInstance().getUniqIDHash());
            OpItem opItem = new OpItem();
            opItem.setKey(key.getData());
            opItem.setLength(i);
            opItem.setNumber(i);
            opItem.setOffset(i);
            opItem.setOp((byte) 0);
            tmpMap.put(key, opItem);
        }

        this.map.putAll(tmpMap);
        iterate(1000);
    }


    @Test
    public void testIterator() throws IOException {
        map = new LRUIndexMap(5000, getPath() + File.separator + getCacheFileName(), true);
        // 插入1万数据，遍历
        for (int i = 0; i < 10000; i++) {
            BytesKey key = new BytesKey(UniqId.getInstance().getUniqIDHash());
            OpItem opItem = new OpItem();
            opItem.setKey(key.getData());
            opItem.setLength(i);
            opItem.setNumber(i);
            opItem.setOffset(i);
            opItem.setOp((byte) 0);
            this.map.put(key, opItem);
        }
        Assert.assertEquals(10000, map.size());

        iterate(10000);
    }


    private void iterate(int size) {
        Iterator<BytesKey> it = map.keyIterator();
        int count = 0;
        while (it.hasNext()) {
            BytesKey key = it.next();
            OpItem item = map.get(key);
            Assert.assertEquals(item.getLength(), item.getNumber());
            it.remove();
            count++;
        }
        Assert.assertEquals(size, count);
        Assert.assertEquals(0, map.size());
    }

    class TestThread extends Thread {
        private CyclicBarrier barrier;
        private AtomicInteger counter;


        public TestThread(CyclicBarrier barrier, AtomicInteger counter) {
            super();
            this.barrier = barrier;
            this.counter = counter;
        }


        @Override
        public void run() {
            try {
                barrier.await();
                List<BytesKey> keys = new ArrayList<BytesKey>();
                List<OpItem> items = new ArrayList<OpItem>();
                for (int i = 0; i < 1000; i++) {
                    final BytesKey key = new BytesKey(UniqId.getInstance().getUniqIDHash());
                    OpItem opItem = new OpItem();
                    opItem.setKey(key.getData());
                    opItem.setLength(2);
                    opItem.setNumber(10);
                    opItem.setOffset(10000);
                    opItem.setOp((byte) 0);
                    map.put(key, opItem);
                    keys.add(key);
                    counter.incrementAndGet();
                    items.add(opItem);
                }
                for (int i = 0; i < keys.size(); i++) {
                    Assert.assertEquals(items.get(i), map.get(keys.get(i)));
                }
                for (int i = 0; i < keys.size(); i++) {
                    map.remove(keys.get(i));
                    Assert.assertNull(map.get(keys.get(i)));
                }
                barrier.await();
            }
            catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        }
    }


    @Test
    public void testConcurrent() throws Exception {
        int threads = 200;
        this.map = new LRUIndexMap(threads / 5 * 1000, getPath() + File.separator + getCacheFileName(), true);
        CyclicBarrier barrier = new CyclicBarrier(threads + 1);
        AtomicInteger counter = new AtomicInteger(0);
        for (int i = 0; i < threads; i++) {
            new TestThread(barrier, counter).start();
        }
        long start = System.currentTimeMillis();
        barrier.await();
        barrier.await();
        System.out.println(threads + "个线程，并发操作" + counter.get() + "个元素，耗时：" + (System.currentTimeMillis() - start));
        Assert.assertEquals(0, map.size());
    }


    @After
    public void tearDown() throws IOException {
        if (this.map != null) {
            this.map.close();
        }
    }
}
