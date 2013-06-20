package edu.uci.ics.genomix.data.test;

import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

public class PositionListWritableTest {

    @Test
    public void TestInitial() {
        PositionListWritable plist = new PositionListWritable();
        Assert.assertEquals(plist.getCountOfPosition(), 0);

        for (int i = 0; i < 200; i++) {
            plist.append(i, (byte) i);
            Assert.assertEquals(i, plist.getPosition(i).getReadID());
            Assert.assertEquals((byte) i, plist.getPosition(i).getPosInRead());
            Assert.assertEquals(i + 1, plist.getCountOfPosition());
        }
        int i = 0;
        for (PositionWritable pos : plist) {
            Assert.assertEquals(i, pos.getReadID());
            Assert.assertEquals((byte) i, pos.getPosInRead());
            i++;
        }
        
        byte [] another = new byte [plist.getLength()*2];
        int start = 20;
        System.arraycopy(plist.getByteArray(), 0, another, start, plist.getLength());
        PositionListWritable plist2 = new PositionListWritable(plist.getCountOfPosition(),another,start);
        for( i = 0; i < plist2.getCountOfPosition(); i++){
            Assert.assertEquals(plist.getPosition(i), plist2.getPosition(i));
        }
    }
    
    @Test
    public void TestRemove() {
        PositionListWritable plist = new PositionListWritable();
        Assert.assertEquals(plist.getCountOfPosition(), 0);
        
        for (int i = 0; i < 5; i++) {
            plist.append(i, (byte) i);
            Assert.assertEquals(i, plist.getPosition(i).getReadID());
            Assert.assertEquals((byte) i, plist.getPosition(i).getPosInRead());
            Assert.assertEquals(i + 1, plist.getCountOfPosition());
        }
        int i = 0;
        for (PositionWritable pos : plist) {
            Assert.assertEquals(i, pos.getReadID());
            Assert.assertEquals((byte) i, pos.getPosInRead());
            i++;
        }
        
        //delete one element each time
        i = 0;
        PositionListWritable copyList = new PositionListWritable();
        copyList.set(plist);
        PositionWritable pos = new PositionWritable();
        Iterator<PositionWritable> iterator;
        for(int j = 0; j < 5; j++){
            iterator = copyList.iterator();
            PositionWritable deletePos = new PositionWritable();
            deletePos.set(j, (byte)j);
            while(iterator.hasNext()){
                pos = iterator.next();
                if(pos.equals(deletePos)){
                    iterator.remove();
                    break;
                }
            }
            Assert.assertEquals(5 - 1 - j, copyList.getCountOfPosition());
            while(iterator.hasNext()){
                pos = iterator.next();
                Assert.assertTrue(pos.getReadID() != deletePos.getReadID());
                Assert.assertTrue(pos.getPosInRead() != deletePos.getPosInRead());
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
