package com.github.zhongl.store.benchmark;

import org.junit.Test;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RandomOptionalFactoryTest {

    @Test
    public void optionsWereCreated() throws Exception {
        AssertCreatedFactory f1 = new AssertCreatedFactory();
        AssertCreatedFactory f2 = new AssertCreatedFactory();

        RandomOptionalFactory<Object> randomOptionalFactory
                = new RandomOptionalFactory<Object>(f1, f2);

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

        RandomOptionalFactory<Object> randomOptionalFactory
                = new RandomOptionalFactory<Object>(f1, f2, f1);

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

        Factory<Object> f1 = new FixInstanceSizeFactory<Object>(5, af1);
        Factory<Object> f2 = new FixInstanceSizeFactory<Object>(5, af2);

        RandomOptionalFactory<Object> randomOptionalFactory =
                new RandomOptionalFactory<Object>(f1, f2, f1);

        for (int i = 0; i < 10; i++) {
            randomOptionalFactory.create();
        }

        af1.assertCreated(5);
        af2.assertCreated(5);
    }


    private static class AssertCreatedFactory implements Factory<Object> {

        private int count;

        @Override
        public Object create() {
            count++;
            return new Object();
        }

        public void assertCreatedGreaterThan(int count) {
            assertThat(this.count, greaterThan(count));
        }

        public void assertCreated(int count) {
            assertThat(this.count, is(count));
        }

        public void assertCreated() {
            assertThat(this.count, greaterThan(0));
        }
    }

}
