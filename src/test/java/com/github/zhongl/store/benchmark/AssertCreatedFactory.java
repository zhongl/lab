package com.github.zhongl.store.benchmark;

import java.util.concurrent.Callable;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class AssertCreatedFactory implements CallableFactory {

    private int count;

    public void assertCreatedGreaterThan(int count) {
        assertThat(this.count, greaterThan(count));
    }

    public void assertCreated(int count) {
        assertThat(this.count, is(count));
    }

    public void assertCreated() {
        assertThat(this.count, greaterThan(0));
    }

    @Override
    public Callable<Object> create() {
        count++;
        return new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return null;
            }
        };
    }
}
