package edu.uci.ics.genomix.type;

import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.util.Marshal;

public class ReadHeadInfoTest {

    @Test
    public void TestInitial() {
        ReadHeadInfo pos = new ReadHeadInfo(0);
        ReadHeadInfo pos1 = new ReadHeadInfo(0);
        byte mateId;
        long readId;
        int posId;
        Random gen = new Random();
        byte[] start = new byte[15];
        for (long i = 0; i < (1 << 47); i++) {
            mateId = (byte) (gen.nextBoolean() ? 1 : 0);
            readId = i;
            posId = (int) (i % (1 << 16));
            pos = new ReadHeadInfo(mateId, readId, posId);
            Assert.assertEquals(pos.getMateId(), mateId);
            Assert.assertEquals(pos.getReadId(), readId);
            Assert.assertEquals(pos.getOffset(), posId);

            long uuid = ((readId + 1) << 17) + ((posId & 0xFFFF) << 1) + (mateId & 0b1);
            Marshal.putLong(uuid, start, 0);
            pos1 = new ReadHeadInfo(uuid);
            Assert.assertEquals(pos1.getMateId(), mateId);
            Assert.assertEquals(pos1.getReadId(), readId + 1);
            Assert.assertEquals(pos1.getOffset(), posId);

            //Assert.assertEquals(pos1.toString(), pos.toString());
        }
    }
}
