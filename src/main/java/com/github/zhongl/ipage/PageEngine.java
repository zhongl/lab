package com.github.zhongl.ipage;

import com.google.common.base.Preconditions;
import com.google.common.cache.*;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class PageEngine extends Engine {

    private static final String PAGE_FILE_EXT = ".page";
    private static final String INDEX_FILE_EXT = ".index";
    private static final int DEFAULT_BACKLOG = 10;
    private static final long DEFAULT_TIMEOUT = 500;
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final Cache<Integer, Page> pages;
    private final File dir;

    private Page appendingPage;
    private ItemIndexFileHashMap currentMap;

    public PageEngine(final File dir) throws IOException {
        super(DEFAULT_TIMEOUT, DEFAULT_TIME_UNIT, DEFAULT_BACKLOG);
        this.dir = dir;
        this.pages = CacheBuilder.newBuilder()
                .concurrencyLevel(1)
                .expireAfterAccess(1, TimeUnit.MINUTES)
                .weakKeys()
                .maximumSize(10)
                .removalListener(new ClosePageOnRemoval())
                .build(new PageCacheLoader());
        createOrLoadDir();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        Closeables.closeQuietly(currentMap);
        pages.cleanUp();
    }

    private void createOrLoadDir() throws IOException {
        if (dir.exists()) {
            throw new UnsupportedOperationException();
            // TODO validate and load
        } else {
            Preconditions.checkArgument(dir.mkdirs(), "Can't create dir %s", dir);
            newAppendingPage();
            newItemIndexFileHashMap(0 + "");
        }
    }

    private void newItemIndexFileHashMap(String name) throws IOException {
        currentMap = new ItemIndexFileHashMap(new File(dir, name + INDEX_FILE_EXT), 4 * 1024 * 100); // TODO refactor init capacity
    }

    private void newAppendingPage() throws IOException {
        Page page = Page.openOn(new File(dir, pages.size() + PAGE_FILE_EXT))
                .createIfNotExist()
                .build();
        this.appendingPage = page;
    }

    // TODO @Count monitor
    // TODO @Elapse monitor
    public boolean append(Item item, FutureCallback<Md5Key> callback) {
        return submit(new Append(item, callback));
    }

    public boolean get(Md5Key itemIndex, FutureCallback<Item> callback) {
        return submit(new Get(itemIndex, callback));
    }

    private static class ClosePageOnRemoval implements RemovalListener<Object, Object> {
        @Override
        public void onRemoval(RemovalNotification<Object, Object> objectObjectRemovalNotification) {
            Object value = objectObjectRemovalNotification.getValue();
            if (value instanceof Page) {
                Page page = (Page) value;
                Closeables.closeQuietly(page);
            }
        }
    }

    private class PageCacheLoader extends CacheLoader<Integer, Page> {

        @Override
        public Page load(Integer key) throws Exception {
            return Page.openOn(new File(dir, key + PAGE_FILE_EXT)).build();
        }
    }

    private class Append extends Task<Md5Key> {

        private final Item item;

        public Append(Item item, FutureCallback<Md5Key> callback) {
            super(callback);
            this.item = item;
        }

        @Override
        protected Md5Key execute() throws IOException {
            Md5Key key = item.md5Key();
            ItemIndex itemIndex = new ItemIndex(0, appendingPage.appender().append(item));
            currentMap.put(key, itemIndex);
            return key;
        }
    }

    private class Get extends Task<Item> {
        private final Md5Key key;

        public Get(Md5Key key, FutureCallback<Item> callback) {
            super(callback);
            this.key = key;
        }

        @Override
        protected Item execute() throws Throwable {
            ItemIndex itemIndex = currentMap.get(key);
            int pageIndex = itemIndex.pageIndex();

            if (isAppendingPage(pageIndex)) return appendingPage.getter().get(itemIndex.offset());

            Page page = pages.get(pageIndex);
            return page.getter().get(itemIndex.offset());
        }

        private boolean isAppendingPage(int pageIndex) {
            return appendingPage.file().getName().equals(pageIndex + PAGE_FILE_EXT);
        }
    }


}
