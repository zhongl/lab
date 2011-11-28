package com.github.zhongl.store;

import com.google.common.base.Throwables;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * {@link Engine} is thread-bound {@link Runnable} executor.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@ThreadSafe
public abstract class Engine {

    public static final Shutdown SHUTDOWN = new Shutdown();

    private final BlockingQueue<Runnable> tasks; // TODO monitor
    private final long timeout;
    private final TimeUnit timeUnit;
    private final Core core;

    public Engine(long timeout, TimeUnit unit, int backlog) {
        this.timeout = timeout;
        this.timeUnit = unit;
        this.tasks = new LinkedBlockingQueue<Runnable>(backlog);
        core = new Core();
    }

    public final void startup() {
        core.start();
    }

    public final void shutdown() {
        try {
            tasks.put(SHUTDOWN);
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }

    public final void awaitForShutdown(long timeout) throws InterruptedException {
        core.join(timeout);
    }

    public final boolean isRunning() {
        return core.isAlive();
    }

    protected final boolean submit(Runnable task) {
        return tasks.offer(task);
    }

    private class Core extends Thread {

        @Override
        public void run() {
            boolean interrupted = false;
            while (true) {
                try {
                    Runnable task = tasks.poll(timeout, timeUnit);
                    if (task instanceof Shutdown) break;
                    task.run();
                } catch (InterruptedException e) {
                    interrupted = true;
                    continue;
                }
            }
            if (interrupted) Thread.currentThread().interrupted();
        }

    }

    private static class Shutdown implements Runnable {
        @Override
        public void run() { }
    }
}