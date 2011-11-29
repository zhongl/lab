package com.github.zhongl.ipage;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class NumberFileNameFilter implements FilenameFilter {
    private static final Pattern CHUNK_NAME_PATTERN = Pattern.compile("[0-9]+");

    @Override
    public boolean accept(File dir, String name) {
        return CHUNK_NAME_PATTERN.matcher(name).matches();
    }
}
