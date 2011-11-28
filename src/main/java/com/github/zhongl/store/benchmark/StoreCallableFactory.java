package com.github.zhongl.store.benchmark;

import com.google.common.base.Preconditions;
import com.taobao.common.store.Store;
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class StoreCallableFactory implements CallableFactory {
    private final Store store;
    private final int valueBytes;
    private final StoreOperations storeOperations;
    private final CallableFactory factory;

    StoreCallableFactory(int valueBytes, Store store, StoreOperations storeOperations) {
        this.valueBytes = valueBytes;
        this.store = store;
        this.storeOperations = storeOperations;
        factory = createRandomOptionalFactory();
    }

    private RandomOptionalFactory createRandomOptionalFactory() {
        CallableFactory fixAddFactory = new FixInstanceSizeFactory(storeOperations.add, new AddFactory());
        CallableFactory fixGetFactory = new FixInstanceSizeFactory(storeOperations.get, new GetFactory());
        CallableFactory fixUpdateFactory = new FixInstanceSizeFactory(storeOperations.update, new UpdateFactory());
        CallableFactory fixRemoveFactory = new FixInstanceSizeFactory(storeOperations.remove, new RemoveFactory());

        List<CallableFactory> options = new ArrayList<CallableFactory>();
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

        return new RandomOptionalFactory(options);
    }

    @Override
    public Callable<?> create() {
        return Preconditions.checkNotNull(factory.create());
    }

    private byte[] randomValue(int salt) {
        byte[] bytes = new byte[valueBytes];
        ByteBuffer.wrap(bytes).putInt(salt);
        return bytes;
    }

    private byte[] md5(byte[] value) {
        return DigestUtils.md5(value);
    }

    private class AddFactory implements CallableFactory {

        private final Random random = new Random(1L);

        @Override
        public Callable<?> create() {
            return new Add(random.nextInt());
        }

    }

    private class GetFactory implements CallableFactory {

        private final Random random = new Random(1L);

        @Override
        public Callable<?> create() {
            return new Get(random.nextInt());
        }

    }

    private class UpdateFactory implements CallableFactory {

        private final Random random = new Random(1L);

        @Override
        public Callable<?> create() {
            return new Update(random.nextInt());
        }

    }

    private class RemoveFactory implements CallableFactory {

        private final Random random = new Random(1L);

        @Override
        public Callable<?> create() {
            return new Remove(random.nextInt());
        }

    }

    private class Add implements Callable {

        protected final byte[] value;
        protected final byte[] key;

        private Add(int i) {

            this.value = randomValue(i);
            this.key = md5(value);
        }

        @Override
        public String toString() {
            return "Add";
        }

        @Override
        public Object call() throws Exception {
            store.add(key, value, true);
            return null;
        }
    }

    private class Update extends Add {

        private Update(int i) {
            super(i);
        }

        @Override
        public Object call() throws Exception {
            return store.update(key, value);
        }

        @Override
        public String toString() {
            return "Update";
        }
    }

    private class Get implements Callable {

        protected final byte[] key;

        private Get(int i) {
            this.key = md5(randomValue(i));
        }

        @Override
        public String toString() {
            return "Get";
        }

        @Override
        public Object call() throws Exception {
            return store.get(key);
        }
    }

    private class Remove extends Get {
        private Remove(int i) {
            super(i);
        }

        @Override
        public String toString() {
            return "Remove";
        }

        @Override
        public Object call() throws Exception {
            return store.remove(key, true);
        }

    }

}
