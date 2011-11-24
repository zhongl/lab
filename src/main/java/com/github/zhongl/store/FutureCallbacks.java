package com.github.zhongl.store;

import com.google.common.util.concurrent.FutureCallback;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FutureCallbacks {

    public static final FutureCallback NONE = new FutureCallback() {
        @Override
        public void onSuccess(Object result) { }

        @Override
        public void onFailure(Throwable t) { }
    };

}
