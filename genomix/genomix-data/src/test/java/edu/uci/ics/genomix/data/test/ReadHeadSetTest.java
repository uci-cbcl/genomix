package edu.uci.ics.genomix.data.test;

import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.ReadHeadSet;
import edu.uci.ics.genomix.type.ReadHeadInfo;

public class ReadHeadSetTest {

    @Test
    public void TestInitial() {
        ReadHeadSet plist = new ReadHeadSet();
        Assert.assertEquals(plist.getCountOfPosition(), 0);
        
        byte mateId;
        long readId;
        int posId;
        for (int i = 0; i < 200; i++) {
            mateId = (byte)1;
            readId = (long)i;
            posId = i;
            plist.append(mateId, readId, posId);
            Assert.assertEquals(plist.getPosition(i).getMateId(), mateId);
            Assert.assertEquals(plist.getPosition(i).getReadId(), readId);
            Assert.assertEquals(plist.getPosition(i).getPosId(), posId);
            Assert.assertEquals(i + 1, plist.getCountOfPosition());
        }
        
        int i = 0;
        for (ReadHeadInfo pos : plist) {
            Assert.assertEquals((byte)1, pos.getMateId());
            Assert.assertEquals((long) i, pos.getReadId());
            Assert.assertEquals(i, pos.getPosId());
            i++;
        }
        
        byte [] another = new byte [plist.getLengthInBytes()*2];
        int start = 20;
        System.arraycopy(plist.getByteArray(), 0, another, start, plist.getLengthInBytes());
        ReadHeadSet plist2 = new ReadHeadSet(another,start);
        for( i = 0; i < plist2.getCountOfPosition(); i++){
            Assert.assertEquals(plist.getPosition(i), plist2.getPosition(i));
        }
    }
    
    @Test
    public void TestRemove() {
        ReadHeadSet plist = new ReadHeadSet();
        Assert.assertEquals(plist.getCountOfPosition(), 0);
        
        byte mateId;
        long readId;
        int posId;
        for (int i = 0; i < 5; i++) {
            mateId = (byte)1;
            readId = (long)i;
            posId = i;
            plist.append(mateId, readId, posId);
            Assert.assertEquals(plist.getPosition(i).getMateId(), mateId);
            Assert.assertEquals(plist.getPosition(i).getReadId(), readId);
            Assert.assertEquals(plist.getPosition(i).getPosId(), posId);
            Assert.assertEquals(i + 1, plist.getCountOfPosition());
        }
        
        int i = 0;
        for (ReadHeadInfo pos : plist) {
            Assert.assertEquals((byte)1, pos.getMateId());
            Assert.assertEquals((long) i, pos.getReadId());
            Assert.assertEquals(i, pos.getPosId());
            i++;
        }
        
        //delete one element each time
        i = 0;
        ReadHeadSet copyList = new ReadHeadSet();
        copyList.set(plist);
        ReadHeadInfo pos = new ReadHeadInfo();
        Iterator<ReadHeadInfo> iterator;
        for(int j = 0; j < 5; j++){
            iterator = copyList.iterator();
            ReadHeadInfo deletePos = new ReadHeadInfo();
            deletePos.set((byte)1, (long)j, j);
            boolean removed = false;
            while(iterator.hasNext()){
                pos = iterator.next();
                if(pos.equals(deletePos)){
                    iterator.remove();
                    removed = true;
                    break;
                }
            }
            Assert.assertTrue(removed);
            Assert.assertEquals(5 - 1 - j, copyList.getCountOfPosition());
            while(iterator.hasNext()){
                pos = iterator.next();
                Assert.assertTrue(! (pos.getUUID() == deletePos.getUUID() && 
                                  pos.getReadId() == deletePos.getReadId() && 
                                  pos.getPosId() == deletePos.getPosId()));
                i++;
            }
        }
        
        //delete all the elements
        i = 0;
        iterator = plist.iterator();
        while(iterator.hasNext()){
            pos = iterator.next();
            iterator.remove();
        }
        
        Assert.assertEquals(0, plist.getCountOfPosition());
    }
}
