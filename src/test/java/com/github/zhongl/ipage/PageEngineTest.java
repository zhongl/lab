package com.github.zhongl.ipage;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static com.github.zhongl.ipage.ItemTest.item;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageEngineTest extends DirBase {
    private PageEngine engine;

    @After
    public void tearDown() throws Exception {
        if (engine != null) {
            engine.shutdown();
            engine.awaitForShutdown(Integer.MAX_VALUE);
        }
    }

    @Test
    public void appendAndget() throws Exception {
        dir = testDir("appendAndget");
        engine = PageEngine.baseOn(dir).build();
        engine.startup();

        Item item = item("item");
        Md5Key key = item.md5Key();

        AssertFutureCallback<Md5Key> md5KeyCallback = new AssertFutureCallback<Md5Key>();
        AssertFutureCallback<Item> itemCallback = new AssertFutureCallback<Item>();

        engine.append(item, md5KeyCallback);
        md5KeyCallback.assertResult(is(key));

        engine.get(key, itemCallback);
        itemCallback.assertResult(is(item));
    }


    @Test
    public void loadExistDir() throws Exception {
        dir = testDir("loadExistDir");

        // create exist dir
        engine = PageEngine.baseOn(dir).pageCapacity(4096).build();
        engine.startup();

        AssertFutureCallback<Md5Key> callback = null;

        for (int i = 0; i < 4; i++) {
            byte[] bytes = new byte[1024];
            ByteBuffer.wrap(bytes).putInt(i);
            callback = new AssertFutureCallback<Md5Key>();
            engine.append(new Item(bytes), callback);
        }

        callback.awaitForDone();
        engine.shutdown();

        // load exist dir
        engine = PageEngine.baseOn(dir).pageCapacity(4096).build();
        engine.startup();

        for (int i = 0; i < 4; i++) {
            byte[] bytes = new byte[1024];
            ByteBuffer.wrap(bytes).putInt(i);
            callback = new AssertFutureCallback<Md5Key>();
            engine.append(new Item(bytes), callback);
        }

        callback.awaitForDone();

        assertThat(new File(dir, 0 + PageEngine.PAGE_FILE_EXT).exists(), is(true));
        assertThat(new File(dir, 1 + PageEngine.PAGE_FILE_EXT).exists(), is(true));
        assertThat(new File(dir, 2 + PageEngine.PAGE_FILE_EXT).exists(), is(true));
    }

    @Test
    public void newItemIndexFileHashMapOnItOverFlow() throws Exception {
        dir = testDir("newItemIndexFileHashMapOnItOverFlow");
        engine = PageEngine.baseOn(dir).initIndexBuckets(1).build();
    }

    @Test
    public void loadExistDirIfThereAreTwoIndexFile() throws Exception {
        // TODO loadExistDirIfThereAreTwoIndexFile
    }

    @Test
    public void flushByInterval() throws Exception {
        // TODO flushByInterval
    }

    @Test
    public void flushByCount() throws Exception {
        // TODO flushByCount
    }

}
