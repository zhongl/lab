package com.github.zhongl.ipage;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ItemTest {

    static Item item(String str) {
        return new Item(str.getBytes());
    }

    @Test
    public void itemToString() throws Exception {
        assertThat(new Item("item".getBytes()).toString(), is("Item{bytes=[105, 116, 101, 109]}"));
    }

}
