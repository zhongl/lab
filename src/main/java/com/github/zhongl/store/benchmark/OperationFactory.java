package com.github.zhongl.store.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.taobao.common.store.Store;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class OperationFactory implements Factory<Runnable> {
    private final Store store;
    private final int valueBytes;
    private final CountDownLatch latch;
    private final Operations operations;
    private final StatisticsCollector statisticsCollector;
    private final Factory<Runnable> factory;

    OperationFactory(int valueBytes, Store store, Operations operations, StatisticsCollector statisticsCollector, CountDownLatch latch) {
        this.valueBytes = valueBytes;
        this.store = store;
        this.operations = operations;
        this.statisticsCollector = statisticsCollector;
        this.latch = latch;
        factory = createRandomOptionalFactory();
    }

    private RandomOptionalFactory<Runnable> createRandomOptionalFactory() {
        Factory<Runnable> fixAddFactory = new FixInstanceSizeFactory<Runnable>(operations.add, new AddFactory());
        Factory<Runnable> fixGetFactory = new FixInstanceSizeFactory<Runnable>(operations.get, new GetFactory());
        Factory<Runnable> fixUpdateFactory = new FixInstanceSizeFactory<Runnable>(operations.update, new UpdateFactory());
        Factory<Runnable> fixRemoveFactory = new FixInstanceSizeFactory<Runnable>(operations.remove, new RemoveFactory());

        List<Factory<Runnable>> options = new ArrayList<Factory<Runnable>>();
        // build ratio options make sure add operation create first.
        options.add(fixAddFactory);
        options.add(fixGetFactory);
        options.add(fixGetFactory);
        options.add(fixAddFactory);
        options.add(fixAddFactory);
        options.add(fixUpdateFactory);
        options.add(fixAddFactory);
        options.add(fixUpdateFactory);
        options.add(fixAddFactory);
        options.add(fixRemoveFactory);

        return new RandomOptionalFactory<Runnable>(options);
    }

    @Override
    public Runnable create() {
        return Preconditions.checkNotNull(factory.create());
    }

    private byte[] md5(byte[] value) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(value);
            return md5.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private class AddFactory implements Factory<Runnable> {
        private final Random random = new Random(1L);

        @Override
        public Runnable create() {
            return new Add(random.nextInt());
        }
    }

    private class GetFactory implements Factory<Runnable> {
        private final Random random = new Random(1L);

        @Override
        public Runnable create() {
            return new Get(random.nextInt());
        }
    }

    private class UpdateFactory implements Factory<Runnable> {
        private final Random random = new Random(1L);

        @Override
        public Runnable create() {
            return new Update(random.nextInt());
        }
    }

    private class RemoveFactory implements Factory<Runnable> {
        private final Random random = new Random(1L);

        @Override
        public Runnable create() {
            return new Remove(random.nextInt());
        }
    }


    private byte[] randomValue(int salt) {
        byte[] bytes = new byte[valueBytes];
        ByteBuffer.wrap(bytes).putInt(salt);
        return bytes;
    }

    abstract class Operation implements Runnable {

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

        private Add(int i) {
            this.value = randomValue(i);
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

        private Update(int i) {super(i);}

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

        private Get(int i) {
            this.key = md5(randomValue(i));
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

    private class Remove extends Get {
        private Remove(int i) {super(i);}

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
