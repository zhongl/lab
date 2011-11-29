package com.github.zhongl.ipage;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class PageEngine extends Engine {

    private static final int DEFAULT_BACKLOG = 10;
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
    private static final long DEFAULT_TIMEOUT = 500L;

    private final List<Page> pages;
    private final File dir;

    private Page appendingPage;
    private ItemIndexFileHashMap currentMap;

    public PageEngine(File dir) throws IOException {
        super(DEFAULT_TIMEOUT, DEFAULT_TIME_UNIT, DEFAULT_BACKLOG);
        this.dir = dir;
        this.pages = new ArrayList<Page>();
        createOrLoadDir();

    }

    @Override
    public void shutdown() {
        super.shutdown();
        Closeables.closeQuietly(currentMap);
        for (Page page : pages) {
            Closeables.closeQuietly(page);
        }
    }

    private void createOrLoadDir() throws IOException {
        if (dir.exists()) {
            throw new UnsupportedOperationException();
            // TODO validate and load
        } else {
            Preconditions.checkArgument(dir.mkdirs(), "Can't create dir %s", dir);
            newPage();
            newItemIndexFileHashMap(0 + "");
        }
    }

    private void newItemIndexFileHashMap(String name) throws IOException {
        currentMap = new ItemIndexFileHashMap(new File(dir, name + ".index"), 4 * 1024 * 100); // TODO refactor init capacity
    }

    private void newPage() throws IOException {
        Page page = Page.openOn(new File(dir, pages.size() + ".page")).createIfNotExist().build();
        this.appendingPage = page;
        pages.add(page);
    }

    // TODO @Count monitor
    // TODO @Elapse monitor
    public boolean append(Item item, FutureCallback<Md5Key> callback) {
        return submit(new Append(item, callback));
    }

    public boolean get(Md5Key itemIndex, FutureCallback<Item> callback) {
        return submit(new Get(itemIndex, callback));
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
            Page page = pages.get(itemIndex.pageIndex());
            return page.getter().get(itemIndex.offset());
        }
    }


}
