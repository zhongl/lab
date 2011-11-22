package com.taobao.store.test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.common.store.journal.Checkpoint;
import com.taobao.common.store.journal.JournalLocation;
import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.UniqId;


public class CheckpointTest {

    private Checkpoint checkPoint;

    private String getPath() {
        return "tmp" + File.separator + "notify-store-test";
    }


    private Map<BytesKey,JournalLocation> getJournalMap(int smallestNumber,int smallestOffset){
        Map<BytesKey,JournalLocation> map = new HashMap<BytesKey,JournalLocation>();
        for(int i =0;i<1000;i++){
            byte[] id = UniqId.getInstance().getUniqIDHash();
            int number = Math.round(1000);
            long offset = Math.round(1000000);
            if(number < smallestNumber && offset < smallestOffset){
                map.put(new BytesKey(id), new JournalLocation(number,offset));
            }
        }
        byte[] id = UniqId.getInstance().getUniqIDHash();
        map.put(new BytesKey(id), new JournalLocation(smallestNumber,smallestOffset));
        return map;
    }


    @Before
    public void setUp(){

        try {
            checkPoint = new Checkpoint(getPath(),getJournalMap(2,100));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheck(){
        checkPoint.check();
        JournalLocation location = checkPoint.getJournalLocation();
        Assert.assertTrue(location != null && location.getNumber() == 2);
        Assert.assertTrue(location.getOffset() == 100);
    }


    @Test
    public void testRecover(){
        try {
            checkPoint.recover();
            Assert.assertEquals(checkPoint.getJournalLocation().number, 2);
            Assert.assertEquals(checkPoint.getJournalLocation().offset, 100);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSaveLocation(){
        try {
            Map<BytesKey,JournalLocation>  map =  getJournalMap(3,100);
            byte[] id = UniqId.getInstance().getUniqIDHash();
            map.put(new BytesKey(id), new JournalLocation(1,12));
            checkPoint.setAddDatas(map);
            checkPoint.check();

            checkPoint.recover();
            Assert.assertEquals(checkPoint.getJournalLocation().number, 1);
            Assert.assertEquals(checkPoint.getJournalLocation().offset, 12);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown(){

    }
}
