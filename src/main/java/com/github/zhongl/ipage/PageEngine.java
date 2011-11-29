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

    static final String PAGE_FILE_EXT = ".page";
    static final String INDEX_FILE_EXT = ".index";
    static final int DEFAULT_BACKLOG = 10;
    static final long DEFAULT_TIMEOUT = 500;
    static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final Cache<Integer, Page> pages;
    private final File dir;
    private final long pageCapacity;
    private int buckets;

    private Page appendingPage;
    private int appendingPageIndex = -1;
    private ItemIndexFileHashMap currentMap;
    private int currentMapIndex = -1;

    public PageEngine(final File dir, long pageCapacity, int initIndexBuckets) throws IOException {
        super(DEFAULT_TIMEOUT, DEFAULT_TIME_UNIT, DEFAULT_BACKLOG);
        this.dir = dir;
        this.pageCapacity = pageCapacity;
        this.buckets = initIndexBuckets;
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
            scanDirAndLoadMaxFileNameIndex();
        } else {
            Preconditions.checkArgument(dir.mkdirs(), "Can't create dir %s", dir);
        }
        openAppendingPage();
        openItemIndexFileHashMap();
    }

    private void scanDirAndLoadMaxFileNameIndex() {
        String[] names = dir.list();
        for (String name : names) {
            if (name.endsWith(INDEX_FILE_EXT)) { // index file
                currentMapIndex = Math.max(currentMapIndex, indexValueOf(name));
            }
            if (name.endsWith(PAGE_FILE_EXT)) { // page file
                appendingPageIndex = Math.max(appendingPageIndex, indexValueOf(name));
            }
        }
    }

    private int indexValueOf(String name) {
        return Integer.parseInt(name.substring(0, name.indexOf('.')));
    }

    private void openItemIndexFileHashMap() throws IOException {
        currentMapIndex++;
        currentMap = new ItemIndexFileHashMap(new File(dir, currentMapIndex + INDEX_FILE_EXT), 4 * 1024 * buckets); // TODO refactor init capacity
        buckets = buckets * 2;
    }

    private void openAppendingPage() throws IOException {
        appendingPageIndex++;
        this.appendingPage = Page.openOn(new File(dir, appendingPageIndex + PAGE_FILE_EXT))
                .createIfNotExist()
                .bytesCapacity(pageCapacity)
                .build();
    }

    // TODO @Count monitor
    // TODO @Elapse monitor
    public boolean append(Record record, FutureCallback<Md5Key> callback) {
        return submit(new Append(record, callback));
    }

    public boolean get(Md5Key itemIndex, FutureCallback<Record> callback) {
        return submit(new Get(itemIndex, callback));
    }

    public static Builder baseOn(File dir) {
        return new Builder(dir);
    }

    public static class Builder {

        private final File dir;
        private long pageCapacity = Page.Builder.DEFAULT_BYTES_CAPACITY;
        private int initIndexBuckets = 100;

        public Builder(File dir) {
            this.dir = dir;
        }

        public Builder pageCapacity(long value) {
            pageCapacity = value;
            return this;
        }

        public Builder initIndexBuckets(int value) {
            // TODO validate value
            initIndexBuckets = value;
            return this;
        }

        public PageEngine build() throws IOException {
            return new PageEngine(dir, pageCapacity, initIndexBuckets);
        }
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

        private final Record record;

        public Append(Record record, FutureCallback<Md5Key> callback) {
            super(callback);
            this.record = record;
        }

        @Override
        protected Md5Key execute() throws IOException {
            Md5Key key = record.md5Key();
            currentMap.put(key, new ItemIndex(appendingPageIndex, doAppend()));
            return key;
        }

        private long doAppend() throws IOException {
            try {
                return appendingPage.append(record);
            } catch (OverflowException e) {
                openAppendingPage();
                return doAppend();
            }
        }
    }

    private class Get extends Task<Record> {
        private final Md5Key key;

        public Get(Md5Key key, FutureCallback<Record> callback) {
            super(callback);
            this.key = key;
        }

        @Override
        protected Record execute() throws Throwable {
            ItemIndex itemIndex = currentMap.get(key);
            int pageIndex = itemIndex.pageIndex();

            if (appendingPageIndex == pageIndex) return appendingPage.get(itemIndex.offset());

            Page page = pages.get(pageIndex);
            return page.get(itemIndex.offset());
        }

    }


}
