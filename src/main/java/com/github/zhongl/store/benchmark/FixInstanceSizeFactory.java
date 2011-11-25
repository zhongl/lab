package com.github.zhongl.store.benchmark;

import java.util.concurrent.atomic.AtomicInteger;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public final class FixInstanceSizeFactory<T> implements Factory<T> {

    private final AtomicInteger count;
    private final Factory<T> delegate;

    public FixInstanceSizeFactory(int size, Factory<T> delegate) {
        this.delegate = delegate;
        count = new AtomicInteger(size);
    }

    @Override
    public T create() {
        return count.decrementAndGet() < 0 ? null : delegate.create();
    }
}
