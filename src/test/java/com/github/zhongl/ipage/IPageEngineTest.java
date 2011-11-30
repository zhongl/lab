package com.github.zhongl.ipage;

import org.junit.After;
import org.junit.Test;

import static com.github.zhongl.ipage.RecordTest.item;
import static org.hamcrest.Matchers.is;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IPageEngineTest extends DirBase {
    private IPageEngine engine;

    @After
    public void tearDown() throws Exception {
        if (engine != null) {
            engine.shutdown();
            engine.awaitForShutdown(Integer.MAX_VALUE);
        }
    }

    @Test
    public void appendAndget() throws Exception {
        dir = testDir("appendAndget");
        engine = IPageEngine.baseOn(dir).build();
        engine.startup();

        Record record = item("record");
        Md5Key key = Md5Key.valueOf(record);

        AssertFutureCallback<Md5Key> md5KeyCallback = new AssertFutureCallback<Md5Key>();
        AssertFutureCallback<Record> itemCallback = new AssertFutureCallback<Record>();

        engine.append(record, md5KeyCallback);
        md5KeyCallback.assertResult(is(key));

        engine.get(key, itemCallback);
        itemCallback.assertResult(is(record));
    }

    @Test
    public void flushByInterval() throws Exception {
        // TODO flushByInterval
    }

    @Test
    public void flushByCount() throws Exception {
        // TODO flushByCount
    }

}
