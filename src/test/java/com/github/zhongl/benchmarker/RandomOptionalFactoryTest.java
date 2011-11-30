package com.github.zhongl.benchmarker;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RandomOptionalFactoryTest {

    private final StatisticsCollector collector = new StatisticsCollector();
    private final CountDownLatch latch = new CountDownLatch(1);

    @Test
    public void optionsWereCreated() throws Exception {
        AssertCreatedFactory f1 = new AssertCreatedFactory();
        AssertCreatedFactory f2 = new AssertCreatedFactory();

        RandomOptionalFactory randomOptionalFactory
                = new RandomOptionalFactory(f1, f2);

        for (int i = 0; i < 10; i++) {
            randomOptionalFactory.create();
        }

        f1.assertCreated();
        f2.assertCreated();
    }

    @Test
    public void optionsWereCreatedByRatio() throws Exception {
        AssertCreatedFactory f1 = new AssertCreatedFactory();
        AssertCreatedFactory f2 = new AssertCreatedFactory();

        RandomOptionalFactory randomOptionalFactory
                = new RandomOptionalFactory(f1, f2, f1);

        for (int i = 0; i < 10; i++) {
            randomOptionalFactory.create();
        }

        f1.assertCreatedGreaterThan(5);
        f2.assertCreatedGreaterThan(2);
    }

    @Test
    public void optionsWereCreatedByFix() throws Exception {
        AssertCreatedFactory af1 = new AssertCreatedFactory();
        AssertCreatedFactory af2 = new AssertCreatedFactory();

        CallableFactory f1 = new FixInstanceSizeFactory(5, af1);
        CallableFactory f2 = new FixInstanceSizeFactory(5, af2);

        RandomOptionalFactory randomOptionalFactory =
                new RandomOptionalFactory(f1, f2, f1);

        for (int i = 0; i < 10; i++) {
            randomOptionalFactory.create();
        }

        af1.assertCreated(5);
        af2.assertCreated(5);
    }


}
