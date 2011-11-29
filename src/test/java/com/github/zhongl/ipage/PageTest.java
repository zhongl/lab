package com.github.zhongl.ipage;

import com.google.common.io.Files;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static com.github.zhongl.ipage.Page.Builder.DEFAULT_BYTES_CAPACITY;
import static com.github.zhongl.ipage.RecordTest.item;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest extends FileBase {
    public static final boolean CLOSE = true;
    public static final boolean FLUSH = false;

    private Page page;

    @Override
    public void tearDown() throws Exception {
        if (page != null) page.close();
        super.tearDown();
    }

    @Test
    public void createPageAndAppendAndClose() throws Exception {
        file = testFile("createAndAppendAndClose");
        assertThat(file.exists(), is(false));
        page = Page.openOn(file).createIfNotExist().build();
        assertAppendAndDurableBy(CLOSE);
    }

    @Test
    public void createPageAndAppendAndFlush() throws Exception {
        file = testFile("createPageAndAppendAndFlush");
        assertThat(file.exists(), is(false));
        page = Page.openOn(file).createIfNotExist().build();
        assertAppendAndDurableBy(FLUSH);
    }

    @Test(expected = OverflowException.class)
    public void appendIfPageHasNotEnoughCapacity() throws Exception {
        file = testFile("appendWhilePageIsFull");
        page = Page.openOn(file).bytesCapacity(12).createIfNotExist().build();
        page.append(item("1234567890"));
    }

    @Test
    public void newBytesCapacityIsNotWorkingForExistPage() throws Exception {
        file = testFile("newBytesCapacityIsNotWorkingForExistPage");
        Page.openOn(file).createIfNotExist().bytesCapacity(12).build();
        page = Page.openOn(file).bytesCapacity(64).build();
        assertThat(page.bytesCapacity(), is(12L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void createPageWithBytesCapacityLessThan12() throws Exception {
        file = testFile("createPageWithBytesCapacityLessThan12");
        Page.openOn(file).createIfNotExist().bytesCapacity(11);
    }

    @Test
    public void appendExistPage() throws Exception {
        file = testFile("appendToExitPage");

        // create a page and append one item
        page = Page.openOn(file).createIfNotExist().build();
        Record record1 = item("record1");
        long offset1 = page.append(record1);
        page.close();

        // open it and append again
        page = Page.openOn(file).build();
        Record record2 = item("record2");
        long offset2 = page.append(record2);

        assertThat(page.get(offset1), is(record1));
        assertThat(page.get(offset2), is(record2));
    }

    @Test
    public void overwriteExistPage() throws Exception {
        file = testFile(" overwriteExistPage");
        Files.append("hi", file, Charset.defaultCharset());

        assertThat(file.exists(), is(true));
        assertThat(file.isFile(), is(true));

        page = Page.openOn(file).overwriteIfExist().build();

        assertAppendAndDurableBy(CLOSE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void openWithNonExistFile() throws Exception {
        file = testFile("openWithNonExistFile");
        page = Page.openOn(file).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void openWithDirectory() throws Exception {
        file = testFile("openWithDirectory");
        file.mkdirs();
        page = Page.openOn(file).build();
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
        assertPageContentOnDiskIs(DEFAULT_BYTES_CAPACITY, "item1".getBytes(), "item2".getBytes());
    }

    private byte[] toBytes(long bytesCapacity, byte[][] items) {
        int lengthBytes = 4;
        int length = 8;

        for (byte[] item : items) {
            length += item.length + lengthBytes;
        }

        byte[] union = new byte[length];

        ByteBuffer buffer = ByteBuffer.wrap(union);

        buffer.putLong(bytesCapacity);
        for (byte[] item : items) {
            buffer.putInt(item.length);
            buffer.put(item);
        }
        return union;
    }


    private void assertPageContentOnDiskIs(long bytesCapacity, byte[]... items) throws IOException {
        assertThat(Files.toByteArray(file), is(toBytes(bytesCapacity, items)));
    }

}
