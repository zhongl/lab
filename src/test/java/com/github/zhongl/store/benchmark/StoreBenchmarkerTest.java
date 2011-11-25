package com.github.zhongl.store.benchmark;

import com.github.zhongl.store.FileBase;
import com.taobao.common.store.Store;
import com.taobao.common.store.journal.JournalStore;
import org.junit.Test;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class StoreBenchmarkerTest extends FileBase {
    private Store store;

    @Override
    public void tearDown() throws Exception {
        store.close();
        super.tearDown();
    }

    @Test
    public void journalStore() throws Exception {
        file = testFile("journalStore");
        String path = file.getParentFile().getPath();
        String name = file.getName();
        boolean force = true;
        boolean enabledIndexLRU = false;

        store = new JournalStore(path, name, force, enabledIndexLRU);
        StoreBenchmarker benchmarker = StoreBenchmarker.of(store)
                .valueBytes(1024)
                .add(10)
                .get(10)
                .update(10)
                .remove(10)
                .concurrent(4)
                .build();
        System.out.println(benchmarker.benchmark());
    }
}
