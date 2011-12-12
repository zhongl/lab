package com.github.zhongl.benchmarker;

import com.google.common.base.Stopwatch;

import java.util.Collection;
import java.util.concurrent.*;

/**
 * {@link Benchmarker} can collect performance benchmarks
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public final class Benchmarker {
    private final ExecutorService executorService;
    private final StatisticsCollector collector;
    private final int times;
    private final CallableFactory callableFactory;
    private final Progress progress;

    public Benchmarker(CallableFactory callableFactory, int concurrent, int times) {
        this.callableFactory = callableFactory;
        this.times = times;
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(256); // avoid memory problem
        ThreadPoolExecutor.CallerRunsPolicy policy = new ThreadPoolExecutor.CallerRunsPolicy();
        executorService = new ThreadPoolExecutor(concurrent, concurrent, 1L, TimeUnit.MINUTES, workQueue, policy);
        progress = new Progress(times, System.out, 1);
        collector = new StatisticsCollector();
    }

    public Collection<Statistics> benchmark() throws InterruptedException {
        collector.start();
        for (int i = 0; i < times; i++) {
            executorService.submit(new Task(callableFactory.create()));
        }

        progress.awaitAndprintStatus();
        executorService.shutdown();
        return collector.haltAndGetStatistics();
    }

    private class Task implements Runnable {

        private final Callable<?> delegate;

        public Task(Callable<?> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void run() {
            Stopwatch stopwatch = new Stopwatch().start();
            String name = delegate.getClass().getName();
            try {
                delegate.call();
                collector.elapse(name, stopwatch.elapsedMillis());
            } catch (Exception e) {
                collector.error(name, e);
            } finally {
                progress.countDown();
            }
        }
    }
}
