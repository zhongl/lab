package com.github.zhongl.ipage.benchmark;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public final class FixInstanceSizeFactory implements CallableFactory {

    private final AtomicInteger count;
    private final CallableFactory delegate;

    public FixInstanceSizeFactory(int size, CallableFactory delegate) {
        this.delegate = delegate;
        count = new AtomicInteger(size);
    }


    @Override
    public Callable<?> create() {
        return count.decrementAndGet() < 0 ? null : delegate.create();
    }
}
