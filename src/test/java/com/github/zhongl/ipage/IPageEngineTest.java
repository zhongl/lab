package com.github.zhongl.ipage;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static com.github.zhongl.ipage.ChunkContentUtils.concatToChunkContentWith;
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
        dir = testDir("flushByInterval");
        File indexFile = new File(new File(dir, IPageEngine.INDEX_DIR), "0");
        File iPageFile = new File(new File(dir, IPageEngine.IPAGE_DIR), "0");

        engine = IPageEngine.baseOn(dir)
                .initBucketSize(1)
                .flushByIntervalMilliseconds(100L).build();
        engine.startup();


        byte[] bytes = "record".getBytes();
        Record record = new Record(bytes);
        engine.append(record);

        Thread.sleep(100L);

        byte[] indexContent = Bytes.concat(new byte[] {1}, md5KeyBytesOf(record), Longs.toByteArray(0L));
        FileContentAsserter.of(indexFile).assertIs(indexContent);
        FileContentAsserter.of(iPageFile).assertIs(concatToChunkContentWith(bytes));
    }

    @Test
    public void flushByCount() throws Exception {
        // TODO flushByCount
    }

    private byte[] md5KeyBytesOf(Record record) {
        byte[] md5bytes = new byte[16];
        Md5Key.valueOf(record).writeTo(ByteBuffer.wrap(md5bytes));
        return md5bytes;
    }

}
