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
        PositionWritable pos1 = new PositionWritable();
        byte mateId;
        long readId;
        int posId;
        Random gen = new Random();
        byte[] start = new byte[15];
        for (long i = 0; i < (1 << 47); i++) {
            mateId = (byte) (gen.nextBoolean() ? 1 : 0);
            readId = i;
            posId = (int) (i % (1 << 16));
            pos = new PositionWritable(mateId, readId, posId);
            Assert.assertEquals(pos.getMateId(), mateId);
            Assert.assertEquals(pos.getReadId(), readId);
            Assert.assertEquals(pos.getPosId(), posId);
            
            long uuid = ((readId + 1) << 17) + ((posId & 0xFFFF) << 1) + (mateId & 0b1);
            Marshal.putLong(uuid, start, 0);
            pos1 = new PositionWritable(start, 0);
            Assert.assertEquals(pos1.getMateId(), mateId);
            Assert.assertEquals(pos1.getReadId(), readId + 1);
            Assert.assertEquals(pos1.getPosId(), posId);
            
            pos.setNewReference(start, 0);
            Assert.assertEquals(pos.getMateId(), mateId);
            Assert.assertEquals(pos.getReadId(), readId + 1);
            Assert.assertEquals(pos.getPosId(), posId);
            
            Assert.assertEquals(pos1.toString(), pos.toString());
            String out = pos.toString();
        }
    }
}
