package com.github.zhongl.store;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static com.github.zhongl.store.Page.Builder.DEFAULT_BYTES_CAPACITY;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest {
    private static final String BASE_ROOT = "target/PageTest/";
    public static final boolean CLOSE = true;
    public static final boolean FLUSH = false;

    private Page page;
    private File file;

    @Before
    public void setUp() throws Exception {
        new File(BASE_ROOT).mkdirs();
    }

    @After
    public void tearDown() throws Exception {
        if (page != null) page.close();
        if (file != null && file.exists()) file.delete();
    }

    @Test
    public void createPageAndAppendAndClose() throws Exception {
        file = testFile("createPageAndAppendAndClose.bin");
        assertThat(file.exists(), is(false));
        page = Page.openOn(file).createIfNotExist().build();
        assertAppendAndDurableBy(CLOSE);
    }

    @Test
    public void createPageAndAppendAndFlush() throws Exception {
        file = testFile("createPageAndAppendAndFlush.bin");
        assertThat(file.exists(), is(false));
        page = Page.openOn(file).createIfNotExist().build();
        assertAppendAndDurableBy(FLUSH);
    }

    @Test(expected = OverflowException.class)
    public void appendIfPageHasNotEnoughCapacity() throws Exception {
        file = testFile("appendWhilePageIsFull.bin");
        page = Page.openOn(file).bytesCapacity(12).createIfNotExist().build();
        page.appender().append(item("1234567890"));
    }

    @Test
    public void newBytesCapacityIsNotWorkingForExistPage() throws Exception {
        file = testFile("newBytesCapacityIsNotWorkingForExistPage.bin");
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
        file = testFile("appendToExitPage.bin");

        // create a page and append one item
        page = Page.openOn(file).createIfNotExist().build();
        Item item1 = item("item1");
        long offset1 = page.appender().append(item1);
        page.close();

        // open it and append again
        page = Page.openOn(file).build();
        Item item2 = item("item2");
        long offset2 = page.appender().append(item2);

        assertThat(page.getter().get(offset1), is(item1));
        assertThat(page.getter().get(offset2), is(item2));
    }

    @Test
    public void overwriteExistPage() throws Exception {
        file = testFile(" overwriteExistPage.bin");
        Files.append("hi", file, Charset.defaultCharset());

        assertThat(file.exists(), is(true));
        assertThat(file.isFile(), is(true));

        page = Page.openOn(file).overwriteIfExist().build();

        assertAppendAndDurableBy(CLOSE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void openWithNonExistFile() throws Exception {
        file = testFile("openWithNonExistFile.bin");
        page = Page.openOn(file).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void openWithDirectory() throws Exception {
        file = testFile("openWithDirectory");
        file.mkdirs();
        page = Page.openOn(file).build();
    }

    private Item item(String str) {
        return new Item(str.getBytes());
    }

    private void assertAppendAndDurableBy(boolean close) throws IOException {
        assertThat(page.appender().append(item("item1")), is(0L));
        assertThat(page.appender().append(item("item2")), is(9L));
        if (close) {
            page.close();
        } else {
            page.appender().flush();
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

    private File testFile(String name) {
        return new File(BASE_ROOT, name);
    }
}
