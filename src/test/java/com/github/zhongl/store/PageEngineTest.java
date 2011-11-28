package com.github.zhongl.store;

import org.junit.Test;

import static com.github.zhongl.store.ItemTest.item;
import static org.hamcrest.Matchers.is;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageEngineTest extends FileBase {
    private PageEngine engine;

    @Override
    public void tearDown() throws Exception {
        if (engine != null) engine.shutdown();
        super.tearDown();
    }

    @Test
    public void appendAndget() throws Exception {
        file = testFile("appendAndget");
        engine = new PageEngine(Page.openOn(file).createIfNotExist().build());
        engine.startup();

        Item item = item("item");
        ItemIndex index = new ItemIndex(0, 0L);

        AssertFutureCallback<ItemIndex> itemIndexCallback = new AssertFutureCallback<ItemIndex>();
        AssertFutureCallback<Item> itemCallback = new AssertFutureCallback<Item>();

        engine.append(item, itemIndexCallback);
        itemIndexCallback.assertResult(is(index));

        engine.get(index, itemCallback);
        itemCallback.assertResult(is(item));
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
