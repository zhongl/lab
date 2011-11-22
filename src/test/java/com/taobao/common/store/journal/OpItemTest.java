package com.taobao.common.store.journal;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public class OpItemTest {
    @Test
    public void equals() throws Exception {
        byte op = OpItem.OP_ADD;
        byte[] key = new byte[]{0};
        int fileno = 1;
        long offset = 0;
        int length = 512;

        OpItem item1 = new OpItem(op, key, fileno, offset, length);
        OpItem item2 = new OpItem(op, key, fileno, offset, length);

        assertThat(item1.hashCode(), is(item2.hashCode()));
        assertThat(item1, is(item2));
    }
}
