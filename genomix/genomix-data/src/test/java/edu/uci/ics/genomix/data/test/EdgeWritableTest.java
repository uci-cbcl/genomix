package edu.uci.ics.genomix.data.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import junit.framework.Assert;
import org.junit.Test;

import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.ReadHeadSet;
import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;

public class EdgeWritableTest {

    /**
     * basic test for append to ReadHeadSet
     */
    @Test
    public void TestInitial() {
        EdgeMap elist = new EdgeMap();
        Assert.assertEquals(0, elist.size());
        Assert.assertEquals(4, elist.getLengthInBytes());
        
        VKmer kmer1 = new VKmer("ACCGCTTAGATACC");
        ReadHeadSet plist1 = new ReadHeadSet();
        plist1.append((byte)1, (long)50, 20);
        plist1.append((byte)0, (long)500, 200);
        plist1.append((byte)1, (long)20, 10);
        
        
        EdgeWritable<VKmer, ReadHeadSet> e1 = new EdgeWritable<VKmer, ReadHeadSet>(kmer1, plist1);

        Assert.assertEquals((byte)1, e1.getValue().getPosition(0).getMateId());
        Assert.assertEquals(50, e1.getValue().getPosition(0).getReadId());
        Assert.assertEquals(20, e1.getValue().getPosition(0).getPosId());
        
        Assert.assertEquals((byte)0, e1.getValue().getPosition(1).getMateId());
        Assert.assertEquals(500, e1.getValue().getPosition(1).getReadId());
        Assert.assertEquals(200, e1.getValue().getPosition(1).getPosId());
        
        Assert.assertEquals((byte)1, e1.getValue().getPosition(2).getMateId());
        Assert.assertEquals(20, e1.getValue().getPosition(2).getReadId());
        Assert.assertEquals(10, e1.getValue().getPosition(2).getPosId());
        
        Assert.assertEquals("ACCGCTTAGATACC", e1.getKey().toString());
        System.out.println(e1.getValue().toString());
//        Assert.assertEquals("{ACCGCTTAGATACC:[20,50,500]}", e1.toString()); // sorted order
    }
    
/*    @Test
    public void TestLists() throws IOException {
        byte mateId;
        long readId;
        int posId;
        VKmer kmer1 = new VKmer("ACCGCTTAGATACC");
        VKmer kmer2 = new VKmer("TACGTACGTAGCTG");
        ReadHeadSet plist1 = new ReadHeadSet();
        ReadHeadSet plist2 = new ReadHeadSet();
        
        EdgeWritable<VKmer, ReadHeadSet> e1 = new EdgeWritable<VKmer, ReadHeadSet>(kmer1, plist1);
        EdgeWritable<VKmer, ReadHeadSet> e2 = new EdgeWritable<VKmer, ReadHeadSet>(kmer2, plist2);
//        EdgeWritable e1 = new EdgeWritable(kmer1, plist1);
//        EdgeWritable e2 = new EdgeWritable(kmer2, plist2);
        
        for (int i = 0; i < 200; i++) {
            mateId = (byte) (i % 2);
            readId = (long)i + 5;
            posId = i + 3;
            e1.getValue().append(readId);
            Assert.assertEquals(0, e1.getValue().getPosition(i).getMateId());
            Assert.assertEquals(readId, e1.getValue().getPosition(i).getReadId());
            Assert.assertEquals(0, e1.getValue().getPosition(i).getPosId());
            if (i % 2 == 0) {
                e2.getValue().append(new ReadHeadInfo(mateId, readId, posId));
                Assert.assertEquals(0, e2.getValue().getPosition(i / 2).getMateId());
                Assert.assertEquals(readId, e2.getValue().getPosition(i / 2).getReadId());
                Assert.assertEquals(0, e2.getValue().getPosition(i / 2).getPosId());
            }
            Assert.assertEquals(i + 1, e1.getValue().getCountOfPosition());
            Assert.assertEquals(i / 2 + 1, e2.getValue().getCountOfPosition());
        }
        Assert.assertEquals("ACCGCTTAGATACC", e1.getKey().toString());
        Assert.assertEquals("TACGTACGTAGCTG", e2.getKey().toString());
        
        int i = 0;
        for (ReadHeadInfo p : e1.getValue()) {
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
    
//    @Test
    public void TestIterator() {
        EdgeMap elist = new EdgeMap();
        VKmer kmer1 = new VKmer("ACCGCTTAGATACC");
        ReadHeadSet plist1 = new ReadHeadSet();
        plist1.append((byte)1, (long)50, 20);
        plist1.append((byte)0, (long)500, 200);
        plist1.append((byte)1, (long)20, 10);
        EdgeWritable<VKmer, ReadHeadSet> e1 = new EdgeWritable<VKmer, ReadHeadSet>(kmer1, plist1);
//        EdgeWritable e1 = new EdgeWritable(kmer1, plist1);
        
        elist.put(e1.getKey(), e1.getValue());
//        elist.add(e1);
        VKmer kmer2 = new VKmer("ATAGCTGAC");
        elist.add(new EdgeWritable(kmer2, plist1));
        
        Iterator<VKmer> keyIter = elist.getKeyIterator();
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
    }*/
}
