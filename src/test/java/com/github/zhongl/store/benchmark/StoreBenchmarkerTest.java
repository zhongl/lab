package com.github.zhongl.store.benchmark;

import com.github.zhongl.store.FileBase;
import com.google.common.io.Files;
import com.taobao.common.store.Store;
import com.taobao.common.store.journal.JournalStore;
import org.junit.Test;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class StoreBenchmarkerTest extends FileBase {
    private Store store;

    @Override
    public void tearDown() throws Exception {
        store.close();
        Files.deleteDirectoryContents(file.getParentFile());
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
        Benchmarker benchmarker = buildBenchmarkerOf(store);
        System.out.println(benchmarker.benchmark());
    }

    private static Benchmarker buildBenchmarkerOf(Store store) {
        StoreOperations storeOperations = new StoreOperations(1000, 1000, 1000, 1000);
        StoreCallableFactory storeOperationFactory = new StoreCallableFactory(1024, store, storeOperations);
        return new Benchmarker(storeOperationFactory, 4, storeOperations.total());
    }


}
