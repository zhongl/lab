package com.taobao.store.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.common.store.journal.JournalStore;
import com.taobao.common.store.util.UniqId;

public class JournalStoreCompactTest {
	JournalStore store = null;


    private String getPath() {
        return "tmp" + File.separator + "notify-store-test";
    }


    private String getStoreName() {
        return "CompactAndRemove";
    }


    @Before
    public void setUp() throws Exception {
    	  String path = getPath();
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

        this.store = new JournalStore(path, getStoreName(),false, false, true);
        assertEquals(0, this.store.size());
    }
    
    @Test
    public void checkDataFile() throws IOException {
    	
    	/**
    	 * 这个测试场景用于测试是否key会被回收，和是否会被整理.
    	 * 
    	 * key1会被删除，key2会被重建, key3会保持不变.
    	 */
    	byte[] key1 = UniqId.getInstance().getUniqIDHash();
    	byte[] key2 = UniqId.getInstance().getUniqIDHash();
    	byte[] key3 = UniqId.getInstance().getUniqIDHash();
        store.add(key1, "OriginalData".getBytes());
        try { Thread.sleep(70); } catch (InterruptedException e) { }
		store.add(key2, "OriginalData".getBytes());
		try { Thread.sleep(150); } catch (InterruptedException e) { }
		store.add(key3, "OriginalData".getBytes());
    	store.setIntervalForCompact(100);
    	store.setIntervalForRemove(200);
    	
    	//key1会被删除，key2会被重建, key3会保持不变.
    	store.check();
    	assertEquals(null, store.get(key1));
    	assertEquals(store.get(key2).length, "OriginalData".getBytes().length);
    	assertEquals(store.get(key3).length, "OriginalData".getBytes().length);
    	
    	try { Thread.sleep(70); } catch (InterruptedException e) { }
    	//key1会被删除，key2会被删除, key3会保持不变.
    	store.check();
    	assertEquals(null, store.get(key1));
    	assertEquals(null, store.get(key2));
    	assertEquals(store.get(key3).length, "OriginalData".getBytes().length);
    }
    
    @After
    public void after() throws IOException {
        if (store != null) {
            store.close();
        }
    }
}
