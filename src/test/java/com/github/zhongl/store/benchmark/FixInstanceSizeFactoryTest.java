package com.github.zhongl.store.benchmark;

import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FixInstanceSizeFactoryTest {

    @Test
    public void returnNullIfOverFixSize() throws Exception {
        Factory<String> stringFactory = new Factory<String>() {
            @Override
            public String create() {
                return "something";
            }
        };
        int size = 5;
        FixInstanceSizeFactory<String> factory = new FixInstanceSizeFactory<String>(size, stringFactory);

        for (int i = 0; i < size; i++) {
            assertThat(factory.create(), is(notNullValue()));
        }
        assertThat(factory.create(), is(nullValue()));
    }
}
