package com.github.zhongl.store;

import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class PageEngine extends Engine {

    private static final int DEFAULT_BACKLOG = 10;
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
    private static final long DEFAULT_TIMEOUT = 500L;

    private final Page page;

    public PageEngine(Page page) {
        this(page, DEFAULT_BACKLOG);
    }

    public PageEngine(Page page, int backlog) {
        super(DEFAULT_TIMEOUT, DEFAULT_TIME_UNIT, backlog);
        this.page = page;
    }

    // TODO @Count monitor
    // TODO @Elapse monitor
    public boolean append(Item item, FutureCallback<ItemIndex> callback) {
        return submit(new Append(item, callback));
    }

    public boolean get(ItemIndex itemIndex, FutureCallback<Item> callback) {
        return submit(new Get(itemIndex, callback));
    }

    private class Append implements Runnable {

        private final Item item;
        private final FutureCallback<ItemIndex> callback;

        public Append(Item item, FutureCallback<ItemIndex> callback) {
            this.item = item;
            this.callback = callback;
        }

        @Override
        public void run() {
            try {
                callback.onSuccess(new ItemIndex(0, page.appender().append(item)));
            } catch (IOException e) {
                callback.onFailure(e);
            }
        }
    }

    private class Get implements Runnable {
        private final ItemIndex itemIndex;
        private final FutureCallback<Item> callback;

        public Get(ItemIndex itemIndex, FutureCallback<Item> callback) {
            this.itemIndex = itemIndex;
            this.callback = callback;
        }

        @Override
        public void run() {
            try {
                callback.onSuccess(page.getter().get(itemIndex.offset()));
            } catch (IOException e) {
                callback.onFailure(e);
            }
        }
    }
}
