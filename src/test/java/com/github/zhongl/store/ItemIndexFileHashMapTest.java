package com.github.zhongl.store;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ItemIndexFileHashMapTest extends FileBase {

    @Test
    public void putAndGet() throws Exception {
        file = testFile("putAndGet");
        int initCapacity = 4 * 1024 * 100; // 400k
        ItemIndexFileHashMap map = new ItemIndexFileHashMap(file, initCapacity);

        Md5Key key = Md5Key.valueOf("key".getBytes());
        ItemIndex itemIndex = new ItemIndex(0, 18L);
        assertThat(map.put(key, itemIndex), is(nullValue()));
        assertThat(map.get(key), is(itemIndex));
    }

    @Test
    public void remove() throws Exception {
        file = testFile("remove");
        int initCapacity = 4 * 1024 * 100; // 400k
        ItemIndexFileHashMap map = new ItemIndexFileHashMap(file, initCapacity);

        Md5Key key = Md5Key.valueOf("key".getBytes());
        ItemIndex itemIndex = new ItemIndex(3, 29L);
        assertThat(map.put(key, itemIndex), is(nullValue()));
        assertThat(map.remove(key), is(itemIndex));
        assertThat(map.get(key), is(nullValue()));
    }
}
