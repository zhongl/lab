package com.github.zhongl.ipage;

import com.github.zhongl.ipage.benchmark.*;
import com.google.common.primitives.Ints;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IPageEngineBenchmark extends DirBase {

    private IPageEngine engine;

    @After
    public void tearDown() throws Exception {
        engine.shutdown();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        dir = testDir("benchmark");
        engine = IPageEngine.baseOn(dir).initBucketSize(100).build();
        engine.startup();
    }

    @Test
    public void benchmark() throws Exception {

        CallableFactory addFactory = new AddFactory(engine);
        CallableFactory getFactory = new GetFactory(engine);
        CallableFactory removeFactory = new RemoveFactory(engine);

        CallableFactory concatCallableFactory = new ConcatCallableFactory(
                new FixInstanceSizeFactory(100, addFactory),
                new FixInstanceSizeFactory(100, getFactory),
                new FixInstanceSizeFactory(100, removeFactory)
        );

        Collection<Statistics> statisticses =
                new Benchmarker(concatCallableFactory, 1, 300).benchmark(); // setup concurrent 1, because engine is not thread-safe
        for (Statistics statisticse : statisticses) {
            System.out.println(statisticse);
        }
    }

    abstract static class OperationFactory implements CallableFactory {
        protected final IPageEngine engine;
        private int count;

        public OperationFactory(IPageEngine engine) {this.engine = engine;}

        protected Record genRecord() {
            return new Record(Ints.toByteArray(count++));
        }
    }

    private static class AddFactory extends OperationFactory {

        public AddFactory(IPageEngine engine) {
            super(engine);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return engine.append(genRecord());
                }
            };
        }

    }

    private static class GetFactory extends OperationFactory {

        public GetFactory(IPageEngine engine) {
            super(engine);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return engine.get(Md5Key.valueOf(genRecord()));
                }
            };
        }

    }

    private static class RemoveFactory extends OperationFactory {

        public RemoveFactory(IPageEngine engine) {
            super(engine);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return engine.remove(Md5Key.valueOf(genRecord()));
                }
            };
        }

    }
}
