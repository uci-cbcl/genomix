package edu.uci.ics.genomix.data.test;

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
    
}
