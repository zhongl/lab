package com.github.zhongl.ipage;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.github.zhongl.ipage.RecordTest.item;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IPageTest extends DirBase {
    public static final boolean CLOSE = true;
    public static final boolean FLUSH = false;

    private IPage page;

    @After
    public void tearDown() throws Exception {
        if (page != null) page.close();
    }

    @Test
    public void createAndAppendAndClose() throws Exception {
        dir = testDir("createAndAppendAndClose");
        assertThat(dir.exists(), is(false));
        page = IPage.baseOn(dir).build();
        assertAppendAndDurableBy(CLOSE);
    }

    @Test
    public void createAndAppendAndFlush() throws Exception {
        dir = testDir("createAndAppendAndFlush");
        assertThat(dir.exists(), is(false));
        page = IPage.baseOn(dir).build();
        assertAppendAndDurableBy(FLUSH);
    }

    @Test
    public void getAfterAppended() throws Exception {
        dir = testDir("getAfterAppended");

        page = IPage.baseOn(dir).build();
        assertThat(page.get(0L), is(nullValue()));

        Record record = item("1");
        long offset = page.append(record);

        assertThat(page.get(offset), is(record));
    }

    @Test
    public void getFromNonAppendingChunk() throws Exception {
        dir = testDir("getFromNonAppendingChunk");
        page = IPage.baseOn(dir).chunkCapacity(4096).build();
        Record record = item("0123456789ab");
        for (int i = 0; i < 257; i++) {
            page.append(record);
        }
        assertThat(new File(dir, "0").exists(), is(true));
        assertThat(new File(dir, "4096").exists(), is(true)); // assert second chunk exist

        assertThat(page.get(0L), is(record));
        assertThat(page.get(4080L), is(record));
        assertThat(page.get(4096L), is(record));
    }

    @Test
    public void truncateByOffset() throws Exception {
        dir = testDir("truncateByOffset");
        page = IPage.baseOn(dir).chunkCapacity(4096).build();

        Record record = item("0123456789ab");
        for (int i = 0; i < 513; i++) {
            page.append(record);
        }

        assertThat(new File(dir, "0").exists(), is(true));
        assertThat(new File(dir, "4096").exists(), is(true));
        assertThat(new File(dir, "8192").exists(), is(true));

        page.truncate(4112L);

        assertThat(new File(dir, "0").exists(), is(false));
        assertThat(new File(dir, "4096").exists(), is(false));
        assertThat(new File(dir, "4112").exists(), is(true)); // assert new chunk
        assertThat(new File(dir, "8192").exists(), is(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidChunkCapacity() throws Exception {
        dir = testDir("invalidChunkCapacity");
        IPage.baseOn(dir).chunkCapacity(4095);
    }

    @Test(expected = IllegalStateException.class)
    public void repeatSetupChunkCapcity() throws Exception {
        dir = testDir("repeatSetupChunkCapcity");
        IPage.baseOn(dir).chunkCapacity(4096).chunkCapacity(1);
    }

    @Test
    public void loadExist() throws Exception {
        dir = testDir("loadExist");

        // create a page with two chunk
        page = IPage.baseOn(dir).build();
        Record record = item("0123456789ab");
        for (int i = 0; i < 257; i++) {
            page.append(record);
        }
        page.close();

        assertThat(new File(dir, "0").exists(), is(true));
        assertThat(new File(dir, "4096").exists(), is(true));

        // load and verify
        page = IPage.baseOn(dir).build();
        Record newRecord = item("newRecord");
        long offset = page.append(newRecord);

        assertThat(page.get(0L), is(record));
        assertThat(page.get(offset), is(newRecord));
    }

    @Test(expected = IllegalStateException.class)
    @Ignore("TODO")
    public void invalidChunkLength() throws Exception {
        // TODO invalidChunkLength
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidDir() throws Exception {
        dir = testDir("invalidDir");
        dir.createNewFile();
        IPage.baseOn(dir);
    }

    @Test
    @Ignore("TODO")
    public void iteratePage() throws Exception {
        // TODO iteratePage
    }

    private void assertAppendAndDurableBy(boolean close) throws IOException {
        assertThat(page.append(item("item1")), is(0L));
        assertThat(page.append(item("item2")), is(9L));
        if (close) {
            page.close();
        } else {
            page.flush();
        }
        assertPageContentOnDiskIs("item1".getBytes(), "item2".getBytes());
    }

    private byte[] toBytes(byte[][] items) {
        int lengthBytes = 4;
        int length = 0;

        for (byte[] item : items) {
            length += item.length + lengthBytes;
        }

        byte[] union = new byte[length];

        ByteBuffer buffer = ByteBuffer.wrap(union);

        for (byte[] item : items) {
            buffer.putInt(item.length);
            buffer.put(item);
        }
        return union;
    }


    private void assertPageContentOnDiskIs(byte[]... items) throws IOException {
        byte[] expect = toBytes(items);
        byte[] actual = new byte[expect.length];
        byte[] all = Files.toByteArray(new File(dir, "0"));
        System.arraycopy(all, 0, actual, 0, actual.length);
        assertThat(actual, is(expect));
    }

}
