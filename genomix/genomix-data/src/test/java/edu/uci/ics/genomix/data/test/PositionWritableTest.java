package edu.uci.ics.genomix.data.test;
import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.type.PositionWritable;

public class PositionWritableTest {

    @Test
    public void TestInitial() {
        PositionWritable pos = new PositionWritable();
        PositionWritable pos1 = new PositionWritable();
        byte mateId;
        long readId;
        int posId;
        byte[] start = new byte[8];
        for (int i = 0; i < 65535; i++) {
            mateId = (byte)1;
            readId = (long)i;
            posId = i;
            pos = new PositionWritable(mateId, readId, posId);
            Assert.assertEquals(pos.getMateId(), mateId);
            Assert.assertEquals(pos.getReadId(), readId);
            Assert.assertEquals(pos.getPosId(), posId);
            
            long finalId = ((readId + 1) << 17) + ((posId & 0xFFFF) << 1) + (mateId & 0b1);
            Marshal.putLong(finalId, start, 0);
            pos1 = new PositionWritable(start, 0);
            Assert.assertEquals(pos1.getMateId(), mateId);
            Assert.assertEquals(pos1.getReadId(), readId + 1);
            Assert.assertEquals(pos1.getPosId(), posId);
            
            pos.setNewReference(start, 0);
            Assert.assertEquals(pos.getMateId(), mateId);
            Assert.assertEquals(pos.getReadId(), readId + 1);
            Assert.assertEquals(pos.getPosId(), posId);
        }
    }
}
