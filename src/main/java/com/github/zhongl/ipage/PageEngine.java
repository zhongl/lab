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

    private class Append implements Runnable {

        private final Item item;
        private final FutureCallback<Md5Key> callback;

        public Append(Item item, FutureCallback<Md5Key> callback) {
            this.item = item;
            this.callback = callback;
        }

        @Override
        public void run() {
            try {
                Md5Key key = item.md5Key();
                ItemIndex itemIndex = new ItemIndex(0, appendingPage.appender().append(item));
                currentMap.put(key, itemIndex);
                callback.onSuccess(key);
            } catch (IOException e) {
                callback.onFailure(e);
            }
        }
    }

    private class Get implements Runnable {
        private final Md5Key key;
        private final FutureCallback<Item> callback;

        public Get(Md5Key key, FutureCallback<Item> callback) {
            this.key = key;
            this.callback = callback;
        }

        @Override
        public void run() {
            try {
                ItemIndex itemIndex = currentMap.get(key);
                Page page = pages.get(itemIndex.pageIndex());
                callback.onSuccess(page.getter().get(itemIndex.offset()));
            } catch (IOException e) {
                callback.onFailure(e);
            }
        }
    }
}
