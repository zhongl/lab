package com.github.zhongl.ipage;

import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RecordIndexTest extends FileBase {

    private RecordIndex map;

    @Override
    public void tearDown() throws Exception {
        if (map != null) map.close();
        super.tearDown();
    }

    @Test
    public void putAndGet() throws Exception {
        file = testFile("putAndGet");
        map = newRecordIndexWithBuckets(1);

        Md5Key key = Md5Key.valueOf("key".getBytes());

        long offset = 7L;
        assertThat(map.put(key, offset), is(nullValue()));
        assertThat(map.get(key), is(offset));
    }

    @Test
    public void remove() throws Exception {
        file = testFile("remove");
        map = newRecordIndexWithBuckets(1);

        Md5Key key = Md5Key.valueOf("key".getBytes());
        Long offset = 7L;
        assertThat(map.put(key, offset), is(nullValue()));
        assertThat(map.remove(key), is(offset));
        assertThat(map.get(key), is(nullValue()));
    }

    @Test
    public void putInReleasedSlot() throws Exception {
        file = testFile("putInReleasedSlot");
        map = newRecordIndexWithBuckets(1);

        fillFullRecordIndex();

        Md5Key key0 = Md5Key.valueOf(Ints.toByteArray(0));
        Md5Key key1 = Md5Key.valueOf(Ints.toByteArray(1));
        map.remove(key0);
        map.remove(key1);
        assertThat(map.put(key1, 7L), is(nullValue()));
        assertThat(map.put(key0, 7L), is(nullValue())); // no exception means new item index put in released slot.
    }

    @Test
    public void getAndRemoveEmplyBucket() throws Exception {
        file = testFile("getAndRemoveEmplyBucket");
        map = newRecordIndexWithBuckets(1);

        Md5Key key = Md5Key.valueOf(Ints.toByteArray(1));
        assertThat(map.get(key), is(nullValue()));
        assertThat(map.remove(key), is(nullValue()));
    }

    @Test
    public void getAndRemoveByInvalidKey() throws Exception {
        file = testFile("getAndRemoveByInvalidKey");
        map = newRecordIndexWithBuckets(1);

        fillFullRecordIndex();

        Md5Key invalidKey = Md5Key.valueOf(Ints.toByteArray(163));

        assertThat(map.get(invalidKey), is(nullValue()));
        assertThat(map.remove(invalidKey), is(nullValue()));
    }

    @Test(expected = IllegalStateException.class)
    public void unknownSlotState() throws Exception {
        file = testFile("unknownSlotState");
        map = newRecordIndexWithBuckets(1);
        Files.write(new byte[] {3}, file);  // write a unknown slot state
        map.get(Md5Key.valueOf(Ints.toByteArray(1))); // trigger exception
    }

    @Test(expected = OverflowException.class)
    public void noSlotForNewItemIndex() throws Exception {
        file = testFile("noSlotForNewItemIndex");
        fillFullRecordIndex();
        Md5Key key163 = Md5Key.valueOf(Ints.toByteArray(163));
        map.put(key163, 7L);  // trigger exception
    }

    private void fillFullRecordIndex() throws Exception {
        map = newRecordIndexWithBuckets(1);
        for (int i = 0; i < 163; i++) {
            map.put(Md5Key.valueOf(Ints.toByteArray(i)), 10L);
        }
    }

    private RecordIndex newRecordIndexWithBuckets(int buckets) throws IOException {
        int initCapacity = 4 * 1024 * buckets;
        return new RecordIndex(file, initCapacity);
    }

}
