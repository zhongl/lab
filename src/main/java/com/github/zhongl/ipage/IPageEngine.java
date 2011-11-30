package com.github.zhongl.ipage;

import com.google.common.io.Closeables;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class IPageEngine extends Engine {

    static final int DEFAULT_BACKLOG = Integer.getInteger("ipage.engine.backlog", 10);
    static final long DEFAULT_TIMEOUT = Long.getLong("ipage.engine.timeout", 500);
    static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final File dir;
    private final IPage ipage;
    private final Index index;

    public IPageEngine(final File dir, int chunkCapacity, int initBucketSize) throws IOException {
        super(DEFAULT_TIMEOUT, DEFAULT_TIME_UNIT, DEFAULT_BACKLOG);
        this.dir = dir;
        ipage = IPage.baseOn(new File(dir, "ipage")).chunkCapacity(chunkCapacity).build();
        index = Index.baseOn(new File(dir, "index")).initBucketSize(initBucketSize).build();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        Closeables.closeQuietly(index);
        Closeables.closeQuietly(ipage);
    }

    public static Builder baseOn(File dir) {
        return new Builder(dir);
    }

    // TODO @Count monitor
    // TODO @Elapse monitor
    public boolean append(Record record, FutureCallback<Md5Key> callback) {
        return submit(new Append(record, callback));
    }

    public Md5Key append(Record record) throws IOException, InterruptedException {
        Sync<Md5Key> callback = new Sync<Md5Key>();
        append(record, callback);
        return callback.get();
    }

    public boolean get(Md5Key key, FutureCallback<Record> callback) {
        return submit(new Get(key, callback));
    }

    public Record get(Md5Key key) throws IOException, InterruptedException {
        Sync<Record> callback = new Sync<Record>();
        get(key, callback);
        return callback.get();
    }

    public boolean remove(Md5Key key, FutureCallback<Record> callback) {
        return submit(new Remove(key, callback));
    }

    public Record remove(Md5Key key) throws IOException, InterruptedException {
        Sync<Record> callback = new Sync<Record>();
        remove(key, callback);
        return callback.get();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("IPageEngine");
        sb.append("{dir=").append(dir);
        sb.append(", ipage=").append(ipage);
        sb.append(", index=").append(index);
        sb.append('}');
        return sb.toString();
    }

    private class Append extends Task<Md5Key> {

        private final Record record;

        public Append(Record record, FutureCallback<Md5Key> callback) {
            super(callback);
            this.record = record;
        }

        @Override
        protected Md5Key execute() throws IOException {
            Md5Key key = Md5Key.valueOf(record);
            index.put(key, ipage.append(record));
            return key;
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
            return ipage.get(index.get(key));
        }

    }

    private class Remove extends Task<Record> {

        private final Md5Key key;

        public Remove(Md5Key key, FutureCallback<Record> callback) {
            super(callback);
            this.key = key;
        }

        @Override
        protected Record execute() throws Throwable {
            Long offset = index.remove(key);
            // TODO use a slide window to truncate iPage.
            return ipage.get(offset);
        }

    }

    public static class Builder {

        private static final int UNSET = -1;
        private final File dir;
        private int chunkCapacity = UNSET;
        private int initBucketSize = UNSET;

        public Builder(File dir) {
            this.dir = dir;
        }

        public Builder chunkCapacity(int value) {
            checkState(chunkCapacity == UNSET, "Chunk capacity can only set once.");
            chunkCapacity = value;
            return this;
        }

        public Builder initBucketSize(int value) {
            checkState(initBucketSize == UNSET, "Initial bucket size can only set once.");
            initBucketSize = value;
            return this;
        }

        public IPageEngine build() throws IOException {
            chunkCapacity = (chunkCapacity == UNSET) ? Chunk.DEFAULT_CAPACITY : chunkCapacity;
            initBucketSize = (initBucketSize == UNSET) ? Buckets.DEFAULT_SIZE : initBucketSize;
            return new IPageEngine(dir, chunkCapacity, initBucketSize);
        }
    }
}
