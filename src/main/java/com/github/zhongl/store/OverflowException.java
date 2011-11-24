package com.github.zhongl.store;

import java.io.IOException;

/**
 * {@link com.github.zhongl.store.OverflowException} means there is no enough remains for new item.
 * <p/>
 * <a href="mailto:zhong.lunfu@gmail.com">zhongl</a>
 */
public class OverflowException extends IOException {

    public OverflowException() {
        super();
    }

    public OverflowException(String message) {
        super(message);
    }

    public OverflowException(String message, Throwable cause) {
        super(message, cause);
    }

    public OverflowException(Throwable cause) {
        super(cause);
    }
}
