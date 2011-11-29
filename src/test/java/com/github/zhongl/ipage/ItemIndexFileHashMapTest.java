package com.github.zhongl.ipage;

import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ItemIndexFileHashMapTest extends FileBase {

    private ItemIndexFileHashMap map;

    @Override
    public void tearDown() throws Exception {
        if (map != null) map.close();
        super.tearDown();
    }

    @Test
    public void putAndGet() throws Exception {
        file = testFile("putAndGet");
        map = newItemIndexFileHashMap(100);

        Md5Key key = Md5Key.valueOf("key".getBytes());
        ItemIndex itemIndex = new ItemIndex(0, 18L);
        assertThat(map.put(key, itemIndex), is(nullValue()));
        assertThat(map.get(key), is(itemIndex));
    }

    @Test
    public void remove() throws Exception {
        file = testFile("remove");
        map = newItemIndexFileHashMap(100);

        Md5Key key = Md5Key.valueOf("key".getBytes());
        ItemIndex itemIndex = new ItemIndex(3, 29L);
        assertThat(map.put(key, itemIndex), is(nullValue()));
        assertThat(map.remove(key), is(itemIndex));
        assertThat(map.get(key), is(nullValue()));
    }

    @Test
    public void putItemIndexInReleasedSlot() throws Exception {
        file = testFile("putItemIndexInReleasedSlot");
        map = newItemIndexFileHashMap(1);

        put141ItemIndexInOneBucket();

        Md5Key key0 = Md5Key.valueOf(Ints.toByteArray(0));
        Md5Key key1 = Md5Key.valueOf(Ints.toByteArray(1));
        ItemIndex itemIndex = new ItemIndex(0, 0L);
        map.remove(key0);
        map.remove(key1);
        assertThat(map.put(key1, itemIndex), is(nullValue()));
        assertThat(map.put(key0, itemIndex), is(nullValue())); // no exception means new item index put in released slot.
    }

    @Test
    public void getAndRemoveEmplyBucket() throws Exception {
        file = testFile("getAndRemoveEmplyBucket");
        map = newItemIndexFileHashMap(1);

        Md5Key key = Md5Key.valueOf(Ints.toByteArray(1));
        assertThat(map.get(key), is(nullValue()));
        assertThat(map.remove(key), is(nullValue()));
    }

    @Test
    public void getAndRemoveByInvalidKey() throws Exception {
        file = testFile("getAndRemoveByInvalidKey");
        map = newItemIndexFileHashMap(1);

        put141ItemIndexInOneBucket();

        Md5Key invalidKey = Md5Key.valueOf(Ints.toByteArray(141));

        assertThat(map.get(invalidKey), is(nullValue()));
        assertThat(map.remove(invalidKey), is(nullValue()));
    }

    @Test(expected = IllegalStateException.class)
    public void unknownSlotState() throws Exception {
        file = testFile("unknownSlotState");
        map = newItemIndexFileHashMap(1);
        Files.write(new byte[] {3}, file);  // write a unknown slot state
        map.get(Md5Key.valueOf(Ints.toByteArray(1))); // trigger exception
    }

    @Test(expected = OverflowException.class)
    public void noSlotForNewItemIndex() throws Exception {
        file = testFile("noSlotForNewItemIndex");
        put141ItemIndexInOneBucket();
        Md5Key key141 = Md5Key.valueOf(Ints.toByteArray(141));
        map.put(key141, new ItemIndex(0, 0L));  // trigger exception
    }

    private void put141ItemIndexInOneBucket() throws Exception {
        map = newItemIndexFileHashMap(1);
        for (int i = 0; i < 141; i++) {
            map.put(Md5Key.valueOf(Ints.toByteArray(i)), new ItemIndex(0, 0L));
        }
    }

    private ItemIndexFileHashMap newItemIndexFileHashMap(int buckets) throws IOException {
        int initCapacity = 4 * 1024 * buckets;
        return new ItemIndexFileHashMap(file, initCapacity);
    }

}
