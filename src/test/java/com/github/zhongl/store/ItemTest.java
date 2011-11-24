package com.github.zhongl.store;

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

    @Test
    public void readFromFile() throws Exception {
        //To change body of created methods use File | Settings | File Templates.
    }
}
