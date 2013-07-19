package edu.uci.ics.genomix.data.test;

import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.type.PositionWritable;

public class PositionWritableTest {

    @Test
    public void TestInitial() {
        PositionWritable pos = new PositionWritable();
        Random r = new Random();
        long uuid = -1; //r.nextLong();
        pos = new PositionWritable(uuid);
        Assert.assertEquals(pos.getUUID(), uuid);
        
        byte[] start = new byte[256];
        Marshal.putLong(uuid, start, 0);
        PositionWritable pos1 = new PositionWritable(start, 0);
        Assert.assertEquals(pos1.getUUID(), uuid);
//       
//        for (int i = 0; i < 128; i++) {
//            Marshal.putLong((long)i, start, i);
//            pos = new PositionWritable(start, i);
//            Assert.assertEquals(pos.getUUID(), (long)i);
//            pos.set((long)-i);
//            Assert.assertEquals(pos.getUUID(), (long)-i);
//            pos.setNewReference(start, i);
//            Assert.assertEquals(pos.getUUID(), (long)-i);
//        }
    }
}
