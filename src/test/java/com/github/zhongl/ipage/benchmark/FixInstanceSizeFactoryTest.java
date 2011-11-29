package com.github.zhongl.ipage.benchmark;

import org.junit.Test;

import java.util.concurrent.Callable;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FixInstanceSizeFactoryTest {

    @Test
    public void returnNullIfOverFixSize() throws Exception {
        CallableFactory runnablefactory = new CallableFactory() {
            @Override
            public Callable<?> create() {
                return new Callable() {
                    @Override
                    public Object call() throws Exception {
                        return null;
                    }
                };
            }
        };
        int size = 5;
        FixInstanceSizeFactory factory = new FixInstanceSizeFactory(size, runnablefactory);

        for (int i = 0; i < size; i++) {
            assertThat(factory.create(), is(notNullValue()));
        }
        assertThat(factory.create(), is(nullValue()));
    }
}
