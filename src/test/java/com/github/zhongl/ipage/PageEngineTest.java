package com.github.zhongl.ipage;

import org.junit.After;
import org.junit.Test;

import static com.github.zhongl.ipage.ItemTest.item;
import static org.hamcrest.Matchers.is;

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
        engine = new PageEngine(dir);
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
    public void newPageOnItOverflow() throws Exception {
        // TODO newPageInPageOverflow
    }

    @Test
    public void newItemIndexFileHashMapOnItOverFlow() throws Exception {
        // TODO newItemIndexFileHashMapOnItOverFlow
    }

    @Test
    public void loadExistDir() throws Exception {
        // TODO loadExistDir
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
