package com.taobao.store.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.common.store.journal.OpItem;
import com.taobao.common.store.journal.impl.OpItemHashMap;
import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.UniqId;


/**
 *
 *
 *
 * @author boyan
 *
 * @since 1.0, 2009-10-20 下午03:28:21
 */

public class OpItemHashMapUnitTest {

    OpItemHashMap map;

    private String path;


    private String getPath() {
        return "tmp" + File.separator + "notify-store-test";
    }


    private String getCacheFileName() {
        return "testIndex";
    }


    @Before
    public void setUp() {
        path = getPath();
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
    public void testCrud() throws IOException {
        this.map = new OpItemHashMap(100, path + File.separator + getCacheFileName(), true);

        for (int i = 0; i < 10000; i++) {
            final BytesKey key = new BytesKey(UniqId.getInstance().getUniqIDHash());
            OpItem opItem = new OpItem();
            opItem.setKey(key.getData());
            opItem.setLength(2);
            opItem.setFileSerialNumber(10);
            opItem.setOffset(10000);
            opItem.setOp((byte) 0);
            Assert.assertNull(this.map.get(key));
            Assert.assertTrue(this.map.put(key, opItem));
            Assert.assertEquals(opItem, this.map.get(key));
            map.remove(key);
            Assert.assertNull(this.map.get(key));
        }

    }


    @Test
    public void testItreator() throws IOException {
        this.map = new OpItemHashMap(20000, path + File.separator + getCacheFileName(), true);
        List<BytesKey> keys = new ArrayList<BytesKey>();
        List<OpItem> items = new ArrayList<OpItem>();
        for (int i = 0; i < 10000; i++) {
            final BytesKey key = new BytesKey(UniqId.getInstance().getUniqIDHash());
            OpItem opItem = new OpItem();
            opItem.setKey(key.getData());
            opItem.setLength(2);
            opItem.setFileSerialNumber(10);
            opItem.setOffset(10000);
            opItem.setOp((byte) 0);
            Assert.assertNull(this.map.get(key));
            if ((this.map.put(key, opItem))) {
                keys.add(key);
                items.add(opItem);
            }
        }

        int count = 0;
        Iterator<BytesKey> it = this.map.iterator();
        while (it.hasNext()) {
            Assert.assertNotNull(it.next());
            it.remove();
            count++;
        }
        Assert.assertFalse(it.hasNext());
        Assert.assertNull(it.next());
        Assert.assertEquals(count, keys.size());
        Assert.assertEquals(0, map.size());
    }


    @Test
    public void testCrudMore() throws IOException {
        this.map = new OpItemHashMap(20000, path + File.separator + getCacheFileName(), true);
        List<BytesKey> keys = new ArrayList<BytesKey>();
        List<OpItem> items = new ArrayList<OpItem>();
        int count = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < 18000; i++) {
            final BytesKey key = new BytesKey(UniqId.getInstance().getUniqIDHash());
            OpItem opItem = new OpItem();
            opItem.setKey(key.getData());
            opItem.setLength(2);
            opItem.setFileSerialNumber(10);
            opItem.setOffset(10000);
            opItem.setOp((byte) 0);
            Assert.assertNull(this.map.get(key));
            if ((this.map.put(key, opItem))) {
                keys.add(key);
                count++;
                items.add(opItem);
            }
        }
        Assert.assertEquals(keys.size(), map.size());
        System.out.println("添加" + count + "个元素耗时：" + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        for (int i = 0; i < keys.size(); i++) {
            Assert.assertEquals(items.get(i), this.map.get(keys.get(i)));
        }
        System.out.println("查询" + count + "个元素耗时：" + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        for (int i = 0; i < keys.size(); i++) {
            this.map.remove(keys.get(i));
            Assert.assertNull(this.map.get(keys.get(i)));
        }
        System.out.println("删除" + count + "个元素耗时：" + (System.currentTimeMillis() - start));
    }


    @After
    public void tearDown() throws IOException {
        if (this.map != null) {
            this.map.close();
        }
    }
}
