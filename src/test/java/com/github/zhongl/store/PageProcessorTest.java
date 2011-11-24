package com.github.zhongl.store;

import org.junit.Test;

import static com.github.zhongl.store.ItemTest.item;
import static org.hamcrest.Matchers.is;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageProcessorTest extends FileBase {
    private PageProcessor processor;

    @Override
    public void tearDown() throws Exception {
        if (processor != null) processor.shutdown();
        super.tearDown();
    }

    @Test
    public void appendAndget() throws Exception {
        file = testFile("appendAndget");
        processor = new PageProcessor(Page.openOn(file).createIfNotExist().build());

        Item item = item("item");
        ItemIndex index = new ItemIndex(file, 0L);

        AssertFutureCallback<ItemIndex> itemIndexCallback = new AssertFutureCallback<ItemIndex>();
        AssertFutureCallback<Item> itemCallback = new AssertFutureCallback<Item>();

        processor.append(item, itemIndexCallback);
        itemIndexCallback.assertResult(is(index));

        processor.get(index, itemCallback);
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
