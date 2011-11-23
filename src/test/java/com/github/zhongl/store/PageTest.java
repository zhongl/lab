package com.github.zhongl.store;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest {
    private static final String BASE_ROOT = "target/PageTest/";

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
    public void appendIfPageHasNotEnoughCapacity() throws Exception {
        file = testFile("appendWhilePageIsFull.bin");
        page = Page.openOn(file).bytesCapacity(9).createIfNotExist().build();
        boolean append = page.appender().append(item("1234567890"));
        assertThat(append, is(false));
    }

    @Test
    public void appendToExistPage() throws Exception {
        file = testFile("appendToExitPage.bin");

        // create a page and append one item
        page = Page.openOn(file).createIfNotExist().build();
        Item item1 = item("item1");
        page.appender().append(item1);
        page.close();

        // open it and append again
        page = Page.openOn(file).build();
        Item item2 = item("item2");
        page.appender().append(item2);

        assertThat(page.itemSize(), is(2));
        assertThat(page.getter().get(0), is(item1));
        assertThat(page.getter().get(1), is(item2));
    }

    @Test
    public void appendToNotExistPage() throws Exception {
        file = testFile("appendToNotExistPage.bin");
        assertThat(file.exists(), is(false));
        page = Page.openOn(file).createIfNotExist().build();
        assertAppendTo(page);
    }

    @Test
    public void overwriteExistPage() throws Exception {
        file = testFile(" overwriteExistPage.bin");
        Files.append("hi", file, Charset.defaultCharset());

        assertThat(file.exists(), is(true));
        assertThat(file.isFile(), is(true));

        page = Page.openOn(file).overwriteIfExist().build();

        assertAppendTo(page);
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

    private void assertAppendTo(Page page) throws IOException {
        assertThat(page.itemSize(), is(0));

        Item item1 = item("item1");
        Item item2 = item("item2");
        Item item3 = item("item3");

        assertThat(page.appender().append(item1), is(true));
        assertThat(page.appender().append(item2), is(true));
        assertThat(page.appender().append(item3), is(true));
        assertThat(page.itemSize(), is(3));
        assertThat(page.getter().get(0), is(item1));
        assertThat(page.getter().get(1), is(item2));
        assertThat(page.getter().get(2), is(item3));
    }

    private Item item(String str) {return new Item(str.getBytes());}

    private File testFile(String child) {return new File(BASE_ROOT, child);}
}
