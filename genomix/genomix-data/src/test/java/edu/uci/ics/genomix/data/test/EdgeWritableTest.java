package edu.uci.ics.genomix.data.test;

import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class EdgeWritableTest {

    @Test
    public void TestInitial() {
        EdgeListWritable elist = new EdgeListWritable();
        Assert.assertEquals(0, elist.size());
        Assert.assertEquals(4, elist.getLengthInBytes());
        
        VKmerBytesWritable kmer1 = new VKmerBytesWritable("ACCGCTTAGATACC");
        PositionListWritable plist1 = new PositionListWritable();
        plist1.append((byte)1, (long)50, 20);
        plist1.append((byte)0, (long)500, 200);
        plist1.append((byte)1, (long)20, 10);
        EdgeWritable e1 = new EdgeWritable(kmer1, plist1);
        
        Assert.assertEquals(50, e1.getReadIDs().getPosition(0).getReadId());
        Assert.assertEquals(0, e1.getReadIDs().getPosition(0).getPosId());
        Assert.assertEquals(0, e1.getReadIDs().getPosition(0).getMateId());
        Assert.assertEquals(500, e1.getReadIDs().getPosition(1).getReadId());
        Assert.assertEquals(0, e1.getReadIDs().getPosition(1).getPosId());
        Assert.assertEquals(0, e1.getReadIDs().getPosition(1).getMateId());
        Assert.assertEquals(20, e1.getReadIDs().getPosition(2).getReadId());
        Assert.assertEquals(0, e1.getReadIDs().getPosition(2).getPosId());
        Assert.assertEquals(0, e1.getReadIDs().getPosition(2).getMateId());
        
        Assert.assertEquals("ACCGCTTAGATACC", e1.getKey().toString());
        Assert.assertEquals("{ACCGCTTAGATACC:[20,50,500]}", e1.toString()); // sorted order
    }
    
    @Test
    public void TestLists() throws IOException {
        byte mateId;
        long readId;
        int posId;
        VKmerBytesWritable kmer1 = new VKmerBytesWritable("ACCGCTTAGATACC");
        VKmerBytesWritable kmer2 = new VKmerBytesWritable("TACGTACGTAGCTG");
        PositionListWritable plist1 = new PositionListWritable();
        PositionListWritable plist2 = new PositionListWritable();
        EdgeWritable e1 = new EdgeWritable(kmer1, plist1);
        EdgeWritable e2 = new EdgeWritable(kmer2, plist2);
        
        for (int i = 0; i < 200; i++) {
            mateId = (byte) (i % 2);
            readId = (long)i + 5;
            posId = i + 3;
            e1.appendReadID(readId);
            Assert.assertEquals(0, e1.getReadIDs().getPosition(i).getMateId());
            Assert.assertEquals(readId, e1.getReadIDs().getPosition(i).getReadId());
            Assert.assertEquals(0, e1.getReadIDs().getPosition(i).getPosId());
            if (i % 2 == 0) {
                e2.appendReadID(new PositionWritable(mateId, readId, posId));
                Assert.assertEquals(0, e2.getReadIDs().getPosition(i / 2).getMateId());
                Assert.assertEquals(readId, e2.getReadIDs().getPosition(i / 2).getReadId());
                Assert.assertEquals(0, e2.getReadIDs().getPosition(i / 2).getPosId());
            }
            Assert.assertEquals(i + 1, e1.getReadIDs().getCountOfPosition());
            Assert.assertEquals(i / 2 + 1, e2.getReadIDs().getCountOfPosition());
        }
        Assert.assertEquals("ACCGCTTAGATACC", e1.getKey().toString());
        Assert.assertEquals("TACGTACGTAGCTG", e2.getKey().toString());
        
        int i = 0;
        for (PositionWritable p : e1.getReadIDs()) {
            Assert.assertEquals((byte)0, p.getMateId());
            Assert.assertEquals((long) i + 5, p.getReadId());
            Assert.assertEquals(0, p.getPosId());
            i++;
        }
        
        byte [] another1 = new byte [e1.getLength()*2];
        int start = 20;
        System.arraycopy(e1.marshalToByteArray(), 0, another1, start, e1.getLength());
        EdgeWritable e3 = new EdgeWritable(another1, start); // reference
        Assert.assertEquals(e1.getKey(), e3.getKey());
        for( i = 0; i < plist2.getCountOfPosition(); i++){
            Assert.assertEquals(e1.getReadIDs().getPosition(i), e3.getReadIDs().getPosition(i));
        }
        
        // overwrite previous, and make copies of the array
        start = 40;
        System.arraycopy(e2.marshalToByteArray(), 0, another1, start, e2.getLength());
        EdgeWritable e4 = new EdgeWritable(new EdgeWritable(another1, start)); // reference
        Assert.assertEquals(e2.getKey(), e4.getKey());
        for( i = 0; i < plist2.getCountOfPosition(); i++){
            Assert.assertEquals(e2.getReadIDs().getPosition(i), e4.getReadIDs().getPosition(i));
        }
    }
    
    @Test
    public void TestIterator() {
        EdgeListWritable elist = new EdgeListWritable();
        VKmerBytesWritable kmer1 = new VKmerBytesWritable("ACCGCTTAGATACC");
        PositionListWritable plist1 = new PositionListWritable();
        plist1.append((byte)1, (long)50, 20);
        plist1.append((byte)0, (long)500, 200);
        plist1.append((byte)1, (long)20, 10);
        EdgeWritable e1 = new EdgeWritable(kmer1, plist1);
        elist.add(e1);
        VKmerBytesWritable kmer2 = new VKmerBytesWritable("ATAGCTGAC");
        elist.add(new EdgeWritable(kmer2, plist1));
        
        Iterator<VKmerBytesWritable> keyIter = elist.getKeyIterator();
        Assert.assertTrue(keyIter.hasNext());
        Assert.assertEquals(kmer1, keyIter.next());
        Assert.assertEquals(kmer2, keyIter.next());
        Assert.assertFalse(keyIter.hasNext());
        keyIter.remove();
        
        long[] expected = {50, 500, 20}, readIDs = elist.get(0).readIDArray();
        Iterator<Long> it = elist.get(0).readIDIter();
        for (int i=0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], readIDs[i]);
            Assert.assertTrue(it.hasNext());
            long actual = it.next();
            Assert.assertEquals(expected[i], actual);
        }
        Assert.assertFalse(it.hasNext());
    }
}
