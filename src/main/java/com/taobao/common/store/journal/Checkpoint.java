package com.taobao.common.store.journal;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.SerializerUtil;

/**
 * 定时更新最小的number和最小的offset
 * @author shuihan
 * @date 2010-9-21
 */
public class Checkpoint{

    private Map<BytesKey,JournalLocation> addDatas ;

    private JournalLocation journalLocation;

    private String path;

    private static final String FILE_NAME = "checkpoint";


    private ScheduledExecutorService scheduleTP = Executors.newSingleThreadScheduledExecutor();

    private static final Logger logger = Logger.getLogger(Checkpoint.class);


    public Checkpoint(String path,Map<BytesKey,JournalLocation> addDatas) throws Exception{
        this.path = path;
        this.addDatas = addDatas;
        this.recover();
        startScheduleCheck();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run()
            {
                scheduleTP.shutdown();
            }
        });
    }



    /**
     * 测试用
     * @param addDatas
     */
    public void setAddDatas(Map<BytesKey,JournalLocation> addDatas){
        this.addDatas = addDatas;
    }


    public JournalLocation getJournalLocation(){
        return this.journalLocation;
    }



    private void startScheduleCheck(){
        scheduleTP.scheduleAtFixedRate(new Runnable(){

            public void run() {
                check();
            }}, 30, 30, TimeUnit.MINUTES);
    }



    public void recover() throws  Exception{
        logger.warn("begin to recover the checkpoint ...");
        File dir = new File(this.path);
        if(!dir.exists()){
            dir.mkdirs();
        }
        File checkPointFile = new File(this.path+File.separator+FILE_NAME);
        if(!checkPointFile.exists()){
            return ;
        }
        FileInputStream input = new FileInputStream(checkPointFile);
        FileChannel channel = input.getChannel();
        ByteBuffer buf = ByteBuffer.allocate((int)checkPointFile.length());
        channel.read(buf, 0);
        if(buf.hasRemaining()){
            logger.warn("读取checkpoint错误！");
            checkPointFile.delete();
            return;
        }
        buf.flip();
        if(buf.remaining() <= 0){
            return ;
        }
        this.journalLocation = (JournalLocation)SerializerUtil.decodeObject(buf.array());

        if(channel != null){
            channel.close();
        }
        if(input != null){
            input.close();
        }

        logger.warn("end to recover the checkpoint,fileSerialNumber="+this.journalLocation.getNumber()+",offset="+this.journalLocation.getOffset());

    }


    public void check(){
        if(this.addDatas == null || this.addDatas.size() == 0){
            return ;
        }
        List<JournalLocation> smallestNumberLocations = new ArrayList<JournalLocation>();
        List<JournalLocation> locations = new ArrayList<JournalLocation>(this.addDatas.values());
        Collections.sort(locations,new Comparator<JournalLocation>(){

            public int compare(JournalLocation o1, JournalLocation o2) {
                if(o1.getNumber() == o2.getNumber()){
                    return 0;
                }
                else if(o1.getNumber() < o2.getNumber()){
                    return -1;
                }
                else {
                    return 1;
                }
            }});

        JournalLocation smallestLocation = locations.iterator().next();
        for (JournalLocation location:locations){
            if(location.getNumber() == smallestLocation.getNumber()){
                smallestNumberLocations.add(location);
            }
        }

        Collections.sort(smallestNumberLocations,new Comparator<JournalLocation>(){

            public int compare(JournalLocation o1, JournalLocation o2) {
                if(o1.getOffset() == o2.getOffset()){
                    return 0;
                }
                else if(o1.getOffset() < o2.getOffset()){
                    return -1;
                }
                else {
                    return 1;
                }
            }});
        this.journalLocation = smallestNumberLocations.iterator().next();

        try {
            saveLoaction();
        }
        catch (Exception e) {
            logger.error("保存 check point 失败!", e);
        }
    }



    public void saveLoaction() throws Exception{
        File checkPointFile = new File(this.path+File.separator+FILE_NAME);
        if(checkPointFile.exists()){
            checkPointFile.delete();
            checkPointFile.createNewFile();
        }
        byte[] datas = SerializerUtil.encodeObject(this.journalLocation);
        ByteBuffer buf = ByteBuffer.wrap(datas);
        FileOutputStream outPut = new FileOutputStream(checkPointFile);
        FileChannel channel = outPut.getChannel();
        int writeBytes = channel.write(buf,0);
        if(writeBytes != datas.length){
            logger.warn("checkpoint 写入磁盘失败!");
        }
        logger.warn("写入checkpoint,fileSerialNumber="+this.journalLocation.number+",offset="+this.journalLocation.offset);
        channel.force(true);
        if(channel != null){
            channel.close();
        }
        if(outPut!= null){
            outPut.close();
        }
    }

}
