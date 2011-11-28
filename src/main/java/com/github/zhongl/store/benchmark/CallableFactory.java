package com.github.zhongl.store.benchmark;

import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface CallableFactory {
    Callable<?> create();
}
