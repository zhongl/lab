package com.github.zhongl.ipage;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.github.zhongl.ipage.ItemTest.item;
import static org.hamcrest.Matchers.is;
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
        Item item = item("1");
        long offset = page.append(item);

        assertThat(page.get(offset), is(item));
    }

    @Test
    public void getFromExist() throws Exception {
        dir = testDir("getFromExist");

        page = IPage.baseOn(dir).build();

        Item item = item("1");
        long offset = page.append(item);
        page.close();

        page = IPage.baseOn(dir).build();
        assertThat(page.get(offset), is(item));
    }

    @Test
    @Ignore("TODO")
    public void removeByOffset() throws Exception {
        // TODO removeByOffset
    }

    @Test
    public void getFromNonAppendingChunk() throws Exception {
        dir = testDir("getFromNonAppendingChunk");
        page = IPage.baseOn(dir).chunkCapacity(4096).build();
        Item item = item("0123456789ab");
        for (int i = 0; i < 257; i++) {
            page.append(item);
        }
        assertThat(new File(dir, "0").exists(), is(true));
        assertThat(new File(dir, "4096").exists(), is(true)); // assert second chunk exist

        assertThat(page.get(0L), is(item));
        assertThat(page.get(4080L), is(item));
        assertThat(page.get(4096L), is(item));
    }


    @Test(expected = IllegalArgumentException.class)
    public void invalidChunkCapacity() throws Exception {
        dir = testDir("invalidChunkCapacity");
        IPage.baseOn(dir).chunkCapacity(4095);
    }

    @Test
    public void appendExist() throws Exception {
        dir = testDir("appendExit");

        // create a page and append one item
        page = IPage.baseOn(dir).build();
        Item item1 = item("item1");
        long offset1 = page.append(item1);
        page.close();

        // open it and append again
        page = IPage.baseOn(dir).build();
        Item item2 = item("item2");
        long offset2 = page.append(item2);

        assertThat(page.get(offset1), is(item1));
        assertThat(page.get(offset2), is(item2));
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
