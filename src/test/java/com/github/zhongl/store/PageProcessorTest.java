package com.github.zhongl.store;

import com.google.common.util.concurrent.FutureCallback;
import org.junit.Test;

import static com.github.zhongl.store.ItemTest.item;
import static org.hamcrest.Matchers.is;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageProcessorTest extends FileBase {
    private PageProcessor processor;

    @Override
    public void tearDown() throws Exception {
        processor.shutdown();
        super.tearDown();
    }

    @Test
    public void append() throws Exception {
        file = testFile("append");
        processor = new PageProcessor(Page.openOn(file).createIfNotExist().build());
        AssertFutureCallback<Long> callback = new AssertFutureCallback<Long>();
        processor.append(item("item"), callback);
        callback.assertResult(is(0L));
    }

    @Test
    public void get() throws Exception {
        // TODO get item by ItemIndex
        file = testFile("get");
        processor = new PageProcessor(Page.openOn(file).createIfNotExist().build());
        Item item = item("item");
        processor.append(item, (FutureCallback<Long>) FutureCallbacks.NONE);
        AssertFutureCallback<Item> callback = new AssertFutureCallback<Item>();
        long offset = 0L;
        processor.get(offset, callback);
        callback.assertResult(is(item));
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
