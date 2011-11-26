package com.github.zhongl.store.benchmark;

import com.taobao.common.store.Store;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link StoreBenchmarker} can collect performance benchmarks
 * of {@link com.taobao.common.store.Store} implement.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public class StoreBenchmarker {
    private final Operations operations;
    private final ExecutorService executorService;
    private final CountDownLatch latch;
    private final StatisticsCollector statisticsCollector;
    private final OperationFactory operationFactory;

    private StoreBenchmarker(Store store, int valueBytes, Operations operations, int concurrent) {
        this.operations = operations;
        executorService = Executors.newFixedThreadPool(concurrent);
        latch = new CountDownLatch(operations.total());
        statisticsCollector = new StatisticsCollector();
        operationFactory = new OperationFactory(valueBytes, store, operations, statisticsCollector, latch);
    }

    public static Builder of(Store store) {
        return new Builder(store);
    }

    public Collection<Statistics> benchmark() throws InterruptedException {
        statisticsCollector.start();
        for (int i = 0; i < operations.total(); i++) {
            executorService.submit(operationFactory.create());
        }
        latch.await();
        executorService.shutdown();
        return statisticsCollector.haltAndGetStatistics();
    }


    public static class Builder {

        private int valueBytes;
        private int concurrent;
        private final Store store;
        private int add;
        private int get;
        private int update;
        private int remove;

        public Builder(Store store) {
            this.store = store;
        }

        public StoreBenchmarker build() {
            return new StoreBenchmarker(store, valueBytes, new Operations(add, get, update, remove), concurrent);
        }

        public Builder valueBytes(int value) {
            this.valueBytes = value;
            return this;
        }

        public Builder concurrent(int value) {
            concurrent = value;
            return this;
        }

        public Builder add(int times) {
            add = times;
            return this;
        }

        public Builder get(int times) {
            get = times;
            return this;
        }

        public Builder update(int times) {
            update = times;
            return this;
        }

        public Builder remove(int times) {
            remove = times;
            return this;
        }
    }

}
