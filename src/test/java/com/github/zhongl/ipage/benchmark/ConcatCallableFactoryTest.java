package com.github.zhongl.ipage.benchmark;

import org.junit.Test;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ConcatCallableFactoryTest {

    @Test
    public void main() throws Exception {
        AssertCreatedFactory af1 = new AssertCreatedFactory();
        AssertCreatedFactory af2 = new AssertCreatedFactory();

        FixInstanceSizeFactory f1 = new FixInstanceSizeFactory(1, af1);
        FixInstanceSizeFactory f2 = new FixInstanceSizeFactory(2, af2);
        ConcatCallableFactory callableFactory = new ConcatCallableFactory(f1, f2);

        callableFactory.create();
        af1.assertCreated();
        callableFactory.create();
        callableFactory.create();
        af1.assertCreated(1);
        af2.assertCreated(2);
    }
}
