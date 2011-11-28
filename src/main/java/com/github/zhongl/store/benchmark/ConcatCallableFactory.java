package com.github.zhongl.store.benchmark;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class ConcatCallableFactory implements CallableFactory {
    private final CallableFactory[] factories;
    private int index;

    public ConcatCallableFactory(CallableFactory... factories) {this.factories = factories;}

    @Override
    public Callable<?> create() {
        if (index < factories.length) {
            Callable<?> callable = factories[index].create();
            if (callable != null) return callable;
            index += 1;
            return create();
        }
        return null;
    }
}
