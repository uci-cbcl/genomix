package edu.uci.ics.genomix.data.types;

import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.data.types.ReadHeadInfo;
import edu.uci.ics.genomix.data.utils.Marshal;

public class ReadHeadInfoTest {

    @Test
    public void TestInitial() {
        ReadHeadInfo pos = new ReadHeadInfo();
        ReadHeadInfo pos1 = new ReadHeadInfo();
        byte mateId;
        long readId;
        byte libraryId;
        int offset;
        byte[] start = new byte[15];
        for (long i = 0; i < ReadHeadInfo.MAX_READID_VALUE; i++) {
            mateId = (byte) (i % (ReadHeadInfo.MAX_MATE_VALUE + 1));
            readId = i;
            offset = (int) (i % (ReadHeadInfo.MAX_OFFSET_VALUE + 1));
            libraryId = (byte) (i % (ReadHeadInfo.MAX_LIBRARY_VALUE + 1));
            pos = new ReadHeadInfo(mateId, libraryId, readId, offset, null, null);
            Assert.assertEquals(pos.getMateId(), mateId);
            Assert.assertEquals(pos.getReadId(), readId);
            Assert.assertEquals(pos.getOffset(), offset);

//            long uuid = ((readId + 1) << 17) + ((offset & 0xFFFF) << 1) + (mateId & 0b1);
//            Marshal.putLong(uuid, start, 0);
//            pos1 = new ReadHeadInfo(uuid);
//            Assert.assertEquals(pos1.getMateId(), mateId);
//            Assert.assertEquals(pos1.getReadId(), readId + 1);
//            Assert.assertEquals(pos1.getOffset(), offset);

            //Assert.assertEquals(pos1.toString(), pos.toString());
        }
    }
}
