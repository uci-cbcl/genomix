package edu.uci.ics.genomix.data.types;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.data.utils.Marshal;

public class ReadIdSetTest {

    /**
     * basic check for adding and reading operation related to ReadIdSet
     * 
     * @throws IOException
     */
    @Test
    public void Test1() {
        VKmerList elist = new VKmerList();
        Assert.assertEquals(0, elist.size());
        Assert.assertEquals(4, elist.getByteArray().length);

        VKmer kmer1 = new VKmer("ACCGCTTAGATACC");
        ReadIdSet plist1 = new ReadIdSet();

        long[] testSet = new long[] { (long) 50, (long) 500, (long) 20 };
        for (int i = 0; i < 3; i++) {
            plist1.add(testSet[i]);
        }

        SimpleEntry<VKmer, ReadIdSet> e1 = new SimpleEntry<VKmer, ReadIdSet>(kmer1, plist1);
        long[] benchmark = new long[] { (long) 20, (long) 50, (long) 500 };
        int i = 0;
        for (long iter : e1.getValue()) {
            Assert.assertEquals(benchmark[i], iter);
            i++;
        }

        Assert.assertEquals("ACCGCTTAGATACC", e1.getKey().toString());
        //        System.out.println(e1.getValue().toString());//TODO wait for new data code
        //        Assert.assertEquals("{ACCGCTTAGATACC:[20,50,500]}", e1.toString()); // sorted order
    }

    /**
     * looping check for adding and reading operation related to ReadIdSet
     */
    @Test
    public void Test2() throws IOException {
        long readId;

        VKmer kmer1 = new VKmer("ACCGCTTAGATACC");
        VKmer kmer2 = new VKmer("TACGTACGTAGCTG");

        ReadIdSet plist1 = new ReadIdSet();
        ReadIdSet plist2 = new ReadIdSet();

        SimpleEntry<VKmer, ReadIdSet> e1 = new SimpleEntry<VKmer, ReadIdSet>(kmer1, plist1);
        SimpleEntry<VKmer, ReadIdSet> e2 = new SimpleEntry<VKmer, ReadIdSet>(kmer2, plist2);

        for (int i = 0; i < 200; i++) {
            readId = (long) i + 5;

            e1.getValue().add(readId);
            Assert.assertEquals(readId, e1.getValue().toArray()[i]);

            if (i % 2 == 0) {
                e2.getValue().add(readId);
                Assert.assertEquals(readId, e1.getValue().toArray()[i]);
            }
            Assert.assertEquals(i + 1, e1.getValue().size());
            Assert.assertEquals(i / 2 + 1, e2.getValue().size());
        }
        Assert.assertEquals("ACCGCTTAGATACC", e1.getKey().toString());
        Assert.assertEquals("TACGTACGTAGCTG", e2.getKey().toString());

        int i = 0;
        for (long p : e1.getValue()) {
            Assert.assertEquals((long) i + 5, p);
            i++;
        }
    }

    /**
     * check for Intersection operation related to ReadIdSet
     */
    @Test
    public void Test3() {
        ReadIdSet plist1 = new ReadIdSet();
        ReadIdSet plist2 = new ReadIdSet();
        ReadIdSet benchmark = new ReadIdSet();
        for (int i = 0; i < 200; i++) {
            plist1.add((long) i);
            plist2.add((long) i + 5);

        }
        for (int i = 0; i + 5 < 200; i++) {
            benchmark.add((long) i + 5);
        }
        ReadIdSet results = ReadIdSet.getIntersection(plist1, plist2);
        Assert.assertEquals(benchmark, results);
    }

    /**
     * check for setAsCopy operation related to ReadIdSet
     */
    @Test
    public void Test4() {
        //*****/
        byte[] data = new byte[84];
        int HEADER_SIZE = 4;
        int ITEM_SIZE = 8;
        Marshal.putInt(10, data, 0);
        for (int i = 0; i < 10; i++) {
            Marshal.putLong(i, data, HEADER_SIZE + ITEM_SIZE * i);
        }
        ReadIdSet results = new ReadIdSet();
        ReadIdSet benchmark = new ReadIdSet();
        results.setAsCopy(data, 0);
        for (int i = 0; i < 10; i++) {
            benchmark.add((long) i);
        }
        Assert.assertEquals(benchmark, results);
    }
}
