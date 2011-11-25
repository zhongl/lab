package com.github.zhongl.store.benchmark;

import com.google.common.base.Stopwatch;
import com.taobao.common.store.Store;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class OperationFactory {
    private final Store store;
    private final int valueBytes;
    private final CountDownLatch latch;
    private final Operations operations;
    private final StatisticsCollector statisticsCollector;
    private final Random random = new Random();

    OperationFactory(int valueBytes, Store store, Operations operations, StatisticsCollector statisticsCollector, CountDownLatch latch) {
        this.valueBytes = valueBytes;
        this.store = store;
        this.operations = operations;
        this.statisticsCollector = statisticsCollector;
        this.latch = latch;
    }

    public Operation create() {
        Operation operation = null;
        int i = random.nextInt(10);
        switch (i) {
            case 1:
                operation = new Remve();
                break;
            case 2:
            case 3:
                operation = new Get();
                break;
            case 4:
            case 5:
                operation = new Update();
                break;
            default:
                operation = operations.addRemainsOrCountDown() ? null : new Add();
        }
        return operation;
    }

    public static final byte[] md5(byte[] value) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(value);
            return md5.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    abstract class Operation implements Runnable {

        private final Random random = new Random(1L);


        protected final byte[] randomValue() {
            byte[] bytes = new byte[valueBytes];
            ByteBuffer.wrap(bytes).putInt(random.nextInt());
            return bytes;
        }

        @Override
        public void run() {
            Stopwatch stopwatch = new Stopwatch().start();
            try {
                execute();
            } catch (Throwable t) {
                statisticsCollector.error(opertionName(), t);
            }
            statisticsCollector.elapse(opertionName(), stopwatch.elapsedMillis());
            latch.countDown();
        }

        protected abstract String opertionName();

        protected abstract void execute() throws Throwable;
    }

    private class Add extends Operation {

        protected final byte[] value;
        protected final byte[] key;

        private Add() {
            this.value = randomValue();
            this.key = md5(value);
        }

        @Override
        protected String opertionName() {
            return "Add";
        }

        @Override
        protected void execute() throws Throwable {
            store.add(key, value, true);
        }
    }

    private class Update extends Add {

        @Override
        protected void execute() throws Throwable {
            store.update(key, value);
        }

        @Override
        protected String opertionName() {
            return "Update";
        }
    }

    private class Get extends Operation {

        protected final byte[] key;

        private Get() {
            this.key = md5(randomValue());
        }

        @Override
        protected String opertionName() {
            return "Get";
        }

        @Override
        protected void execute() throws Throwable {
            store.get(key);
        }
    }

    private class Remve extends Get {
        @Override
        protected String opertionName() {
            return "Remove";
        }

        @Override
        protected void execute() throws Throwable {
            store.remove(key, true);
        }
    }

}
