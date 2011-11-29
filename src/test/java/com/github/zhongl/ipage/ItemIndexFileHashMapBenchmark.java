package com.github.zhongl.ipage;

import com.github.zhongl.ipage.benchmark.*;
import com.google.common.primitives.Ints;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ItemIndexFileHashMapBenchmark extends FileBase {

    private ItemIndexFileHashMap map;

    @Override
    public void tearDown() throws Exception {
        if (map != null) map.close();
        super.tearDown();
    }

    @Test
    public void benchmark() throws Exception {
        file = testFile("benchmark");
        int initCapacity = 4 * 1024 * 100;
        map = new ItemIndexFileHashMap(file, initCapacity);

        CallableFactory addFactory = new AddFactory(map);
        CallableFactory replaceFactory = new ReplaceFactory(map);
        CallableFactory getFactory = new GetFactory(map);
        CallableFactory removeFactory = new RemoveFactory(map);


        CallableFactory concatCallableFactory = new ConcatCallableFactory(
                new FixInstanceSizeFactory(100, addFactory),
                new FixInstanceSizeFactory(100, replaceFactory),
                new FixInstanceSizeFactory(100, getFactory),
                new FixInstanceSizeFactory(100, removeFactory)
        );

        Collection<Statistics> statisticses =
                new Benchmarker(concatCallableFactory, 1, 400).benchmark(); // setup concurrent 1, because map is not thread-safe
        for (Statistics statisticse : statisticses) {
            System.out.println(statisticse);
        }
    }

    abstract class OperationFactory implements CallableFactory {
        protected final ItemIndexFileHashMap map;
        private int count;

        public OperationFactory(ItemIndexFileHashMap map) {this.map = map;}

        protected byte[] genKey() {
            return Ints.toByteArray(count++);
        }
    }

    private class AddFactory extends OperationFactory {

        public AddFactory(ItemIndexFileHashMap map) {
            super(map);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return map.put(Md5Key.valueOf(genKey()), genItemIndex());
                }
            };
        }

        protected ItemIndex genItemIndex() {
            return new ItemIndex(0, 0L);
        }

    }

    private class ReplaceFactory extends AddFactory {

        public ReplaceFactory(ItemIndexFileHashMap map) {
            super(map);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return map.put(Md5Key.valueOf(genKey()), genItemIndex());
                }
            };
        }


    }

    private class GetFactory extends OperationFactory {

        public GetFactory(ItemIndexFileHashMap map) {
            super(map);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return map.get(Md5Key.valueOf(genKey()));
                }
            };
        }

    }

    private class RemoveFactory extends OperationFactory {

        public RemoveFactory(ItemIndexFileHashMap map) {
            super(map);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return map.remove(Md5Key.valueOf(genKey()));
                }
            };
        }

    }
}
