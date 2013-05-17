package edu.uci.ics.genomix.data.test;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.type.PositionWritable;

public class PositionWritableTest {

    @Test
    public void TestInitial() {
        PositionWritable pos = new PositionWritable();
        pos = new PositionWritable(3, (byte) 1);
        Assert.assertEquals(pos.getReadID(), 3);
        Assert.assertEquals(pos.getPosInRead(), 1);

        byte[] start = new byte[256];
        for (int i = 0; i < 128; i++) {
            Marshal.putInt(i, start, i);
            start[i + PositionWritable.INTBYTES] = (byte) (i / 2);
            pos = new PositionWritable(start, i);
            Assert.assertEquals(pos.getReadID(), i);
            Assert.assertEquals(pos.getPosInRead(), (byte) (i / 2));
            pos.set(-i, (byte) (i / 4));
            Assert.assertEquals(pos.getReadID(), -i);
            Assert.assertEquals(pos.getPosInRead(), (byte) (i / 4));
            pos.setNewReference(start, i);
            Assert.assertEquals(pos.getReadID(), -i);
            Assert.assertEquals(pos.getPosInRead(), (byte) (i / 4));

        }
    }
}
