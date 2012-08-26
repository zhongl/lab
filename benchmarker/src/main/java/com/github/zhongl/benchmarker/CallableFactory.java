package com.github.zhongl.benchmarker;

import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface CallableFactory {
    Callable<?> create();
}
