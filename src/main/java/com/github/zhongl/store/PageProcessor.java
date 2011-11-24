package com.github.zhongl.store;

import com.google.common.util.concurrent.FutureCallback;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageProcessor {

    private static final int DEFAULT_BACKLOG = 10;
    private final BlockingQueue<Runnable> tasks;
    private final Page page;
    private volatile boolean running = true;

    public PageProcessor(Page page) {
        this(page, DEFAULT_BACKLOG);
    }

    public PageProcessor(Page page, int backlog) {
        this.page = page;
        tasks = new LinkedBlockingQueue<Runnable>(backlog);
        new Engine().start();
    }

    public void shutdown() {
        running = false;
    }

    public void append(Item item, FutureCallback<Long> callback) {
        tasks.offer(new Append(item, callback));
    }

    public void get(long offset, FutureCallback<Item> callback) {
        tasks.offer(new Get(offset, callback));
    }

    private class Engine extends Thread {

        @Override
        public void run() {
            while (running) {
                try {
                    Runnable task = tasks.poll(500, TimeUnit.MILLISECONDS);
                    task.run();
                } catch (InterruptedException e) {
                    // continue
                }
            }
        }

    }

    private class Append implements Runnable {

        private final Item item;
        private final FutureCallback<Long> callback;

        public Append(Item item, FutureCallback<Long> callback) {
            this.item = item;
            this.callback = callback;
        }

        @Override
        public void run() {
            try {
                callback.onSuccess(page.appender().append(item));
            } catch (IOException e) {
                callback.onFailure(e);
            }
        }
    }

    private class Get implements Runnable {
        private final long offset;
        private final FutureCallback<Item> callback;

        public Get(long offset, FutureCallback<Item> callback) {
            this.offset = offset;
            this.callback = callback;
        }

        @Override
        public void run() {
            try {
                callback.onSuccess(page.getter().get(offset));
            } catch (IOException e) {
                callback.onFailure(e);
            }
        }
    }
}
