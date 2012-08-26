package com.github.zhongl.benchmarker;

import org.junit.Test;

import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BenchmarkerTest {

    @Test
    public void benchmark() throws Exception {
        CallableFactory callableFactory = new CallableFactory() {
            @Override
            public Callable<?> create() {
                return  new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        Thread.sleep(50L);
                        return null;
                    }
                }  ;
            }
        };
        new Benchmarker(callableFactory, 1, 100).benchmark();
    }

    public static void main(String[] args) throws Exception {
        new BenchmarkerTest().benchmark();
    }
}
