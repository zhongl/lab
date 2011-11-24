package com.github.zhongl.store;

import java.io.File;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ItemIndex {
    private final File file;
    private final long offset;

    public ItemIndex(File file, long offset) {
        this.file = file;
        this.offset = offset;
    }

    public long offset() {
        return offset;
    }
}
