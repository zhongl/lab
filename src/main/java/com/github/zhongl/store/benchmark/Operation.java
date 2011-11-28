package com.github.zhongl.store.benchmark;

import com.google.common.base.Stopwatch;

import java.util.concurrent.CountDownLatch;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Operation implements Runnable {
    private final StatisticsCollector collector;
    private final CountDownLatch latch;

    Operation(StatisticsCollector collector, CountDownLatch latch) {
        this.latch = latch;
        this.collector = collector;
    }

    @Override
    public final void run() {
        Stopwatch stopwatch = new Stopwatch().start();
        try {
            execute();
        } catch (Throwable t) {
            collector.error(opertionName(), t);
        }
        collector.elapse(opertionName(), stopwatch.elapsedMillis());
        latch.countDown();

    }

    protected abstract void execute() throws Throwable;

    protected abstract String opertionName();
}
