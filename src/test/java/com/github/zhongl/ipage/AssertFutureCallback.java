package com.github.zhongl.ipage;

import com.google.common.util.concurrent.FutureCallback;
import org.hamcrest.Matcher;

import java.util.concurrent.CountDownLatch;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class AssertFutureCallback<T> implements FutureCallback<T> {

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile T result;
    private volatile Throwable t;

    @Override
    public void onSuccess(T result) {
        this.result = result;
        latch.countDown();
    }

    @Override
    public void onFailure(Throwable t) {
        this.t = t;
        latch.countDown();
    }

    public void assertResult(Matcher<T> matcher) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        if (t != null) throw new AssertionError(t);
        matcher.matches(result);
    }
}
