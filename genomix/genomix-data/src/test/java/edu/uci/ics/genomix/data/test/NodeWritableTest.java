package edu.uci.ics.genomix.data.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.data.KmerUtil;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class NodeWritableTest {

    @Test
    public void TestMergeRF_FF() throws IOException {
        KmerBytesWritable.setGlobalKmerLength(5);
        String test1 = "TAGAT"; // rc = ATCTA
        String test2 = "TCTAG"; // rc = CTAGA
        String test3 = "CTAGC"; // rc = GCTAG
        VKmerBytesWritable k1 = new VKmerBytesWritable();
        VKmerBytesWritable k2 = new VKmerBytesWritable();
        VKmerBytesWritable k3 = new VKmerBytesWritable();
        k1.setByRead(5, test1.getBytes(), 0);
        k2.setByRead(5, test2.getBytes(), 0);
        k3.setByRead(5, test3.getBytes(), 0);
        //        k2.mergeWithRFKmer(5, k1);
        //        Assert.assertEquals("ATCTAG", k2.toString());
        //        k2.mergeWithFFKmer(5, k3);
        //        Assert.assertEquals("ATCTAGC", k2.toString());

        PositionWritable read1 = new PositionWritable((byte) 1, 50, 0);
        PositionWritable read2 = new PositionWritable((byte) 1, 75, 0);
        PositionWritable read3 = new PositionWritable((byte) 0, 100, 0);
        PositionListWritable plist1 = new PositionListWritable(Arrays.asList(read1));
        PositionListWritable plist2 = new PositionListWritable();
        PositionListWritable plist3 = new PositionListWritable(Arrays.asList(read3));

        // k1 {r50} --RF-> k2 {r75} --FF-> k3 {~r100}

        NodeWritable n1 = new NodeWritable();
        n1.setInternalKmer(k1);
        n1.setAvgCoverage(10);
        n1.getStartReads().append(read1);
        n1.getEdgeList(DirectionFlag.DIR_RF).add(new EdgeWritable(k2, plist1));
        Assert.assertEquals("(50-0_0)", n1.getEdgeList(DirectionFlag.DIR_RF).get(0).getReadIDs().getPosition(0)
                .toString());
        Assert.assertEquals(10f, n1.getAvgCoverage());

        NodeWritable n2 = new NodeWritable();
        n2.setInternalKmer(k2);
        n2.setAvgCoverage(20);
        n2.getStartReads().append(read2);
        Assert.assertEquals(1, n2.getStartReads().getCountOfPosition());
        n2.getEdgeList(DirectionFlag.DIR_RF).add(new EdgeWritable(k1, plist1));
        n2.getEdgeList(DirectionFlag.DIR_FF).add(new EdgeWritable(k3, plist3));
        Assert.assertEquals(20f, n2.getAvgCoverage());

        NodeWritable n3 = new NodeWritable();
        n3.setInternalKmer(k3);
        n3.setAvgCoverage(30);
        n3.getEndReads().append(read3);
        n3.getEdgeList(DirectionFlag.DIR_RR).add(new EdgeWritable(k2, plist3));
        Assert.assertEquals("(100-0_0)", n3.getEdgeList(DirectionFlag.DIR_RR).get(0).getReadIDs().getPosition(0)
                .toString());
        Assert.assertEquals(30f, n3.getAvgCoverage());

        // dump and recover each
        byte[] block = new byte[2000];
        int offset = 50;
        System.arraycopy(n1.marshalToByteArray(), 0, block, offset, n1.getSerializedLength());
        NodeWritable copy = new NodeWritable(block, offset);
        Assert.assertEquals(n1, copy);
        offset += copy.getSerializedLength();

        System.arraycopy(n2.marshalToByteArray(), 0, block, offset, n2.getSerializedLength());
        copy = new NodeWritable(block, offset);
        Assert.assertEquals(n2, copy);
        offset += copy.getSerializedLength();

        System.arraycopy(n3.marshalToByteArray(), 0, block, offset, n3.getSerializedLength());
        copy = new NodeWritable(block, offset);
        Assert.assertEquals(n3, copy);
        offset += copy.getSerializedLength();

        // merge k1 with k2, then k1k2 with k3
        //      k2.mergeWithRFKmer(5, k1);
        //      Assert.assertEquals("ATCTAG", k2.toString());
        //      k2.mergeWithFFKmer(5, k3);
        //      Assert.assertEquals("ATCTAGC", k2.toString());
        n2.mergeWithNode(DirectionFlag.DIR_RF, n1);
        Assert.assertEquals("ATCTAG", n2.getInternalKmer().toString());
        Assert.assertEquals(15f, n2.getAvgCoverage());
        Assert.assertEquals(1, n2.getEndReads().getCountOfPosition());
        Assert.assertEquals("(50-0_1)", n2.getEndReads().getPosition(0).toString());
        Assert.assertEquals(1, n2.getStartReads().getCountOfPosition());
        Assert.assertEquals("(75-1_1)", n2.getStartReads().getPosition(0).toString());
        Assert.assertEquals(0, n2.inDegree());
        Assert.assertEquals(1, n2.outDegree());
        Assert.assertEquals(k3, n2.getEdgeList(DirectionFlag.DIR_FF).get(0).getKey());

        n2.mergeWithNode(DirectionFlag.DIR_FF, n3);
        Assert.assertEquals("ATCTAGC", n2.getInternalKmer().toString());
        Assert.assertEquals(20f, n2.getAvgCoverage());
        Assert.assertEquals(2, n2.getEndReads().getCountOfPosition());
        Assert.assertEquals("(50-0_1)", n2.getEndReads().getPosition(0).toString());
        Assert.assertEquals("(100-2_0)", n2.getEndReads().getPosition(1).toString());
        Assert.assertEquals(1, n2.getStartReads().getCountOfPosition());
        Assert.assertEquals("(75-1_1)", n2.getStartReads().getPosition(0).toString());
        Assert.assertEquals(0, n2.inDegree());
        Assert.assertEquals(0, n2.outDegree());
    }

    @Test
    public void TestGraphBuildNodes() throws IOException {
        KmerBytesWritable.setGlobalKmerLength(5);
        String test1 = "TAGAT"; // rc = ATCTA
        String test2 = "TCTAG"; // rc = CTAGA
        String test3 = "CTAGC"; // rc = GCTAG
        VKmerBytesWritable k1 = new VKmerBytesWritable();
        VKmerBytesWritable k2 = new VKmerBytesWritable();
        VKmerBytesWritable k3 = new VKmerBytesWritable();
        k1.setByRead(5, test1.getBytes(), 0);
        k2.setByRead(5, test2.getBytes(), 0);
        k3.setByRead(5, test3.getBytes(), 0);
        //            k2.mergeWithRFKmer(5, k1);
        //            Assert.assertEquals("ATCTAG", k2.toString());
        //            k2.mergeWithFFKmer(5, k3);
        //            Assert.assertEquals("ATCTAGC", k2.toString());

        PositionWritable read1 = new PositionWritable((byte) 1, 50, 0);
        PositionWritable read2 = new PositionWritable((byte) 1, 75, 0);
        PositionWritable read3 = new PositionWritable((byte) 0, 100, 0);
        PositionListWritable plist1 = new PositionListWritable(Arrays.asList(read1, read2));
        PositionListWritable plist2 = new PositionListWritable(Arrays.asList(read2, read3));
        PositionListWritable plist3 = new PositionListWritable(Arrays.asList(read3));

        // k1 {r50} --RF-> k2 {r75} --FF-> k3 {~r100}

        // graphbuilding-like merge of n1 and n2
        NodeWritable n1 = new NodeWritable();
        n1.setInternalKmer(k1);
        n1.setAvgCoverage(10);
        n1.getStartReads().append(read1);
        n1.getEdgeList(DirectionFlag.DIR_RF).add(new EdgeWritable(k2, plist1));
        NodeWritable n1_2 = new NodeWritable();  // duplicate node which should end up union'ed in
        n1_2.setInternalKmer(k1);
        n1_2.setAvgCoverage(10);
        n1_2.getStartReads().append(read1);
        n1_2.getEdgeList(DirectionFlag.DIR_RF).add(new EdgeWritable(k3, plist3));
        n1_2.getEdgeList(DirectionFlag.DIR_RF).add(new EdgeWritable(k2, plist2));
        
        // take the union of the edges (a la graphbuilding)
        n1.getEdgeList(DirectionFlag.DIR_RF).unionUpdate(n1_2.getEdgeList(DirectionFlag.DIR_RF));
        
        // the union-update may change the ordering of the original list (what a pain!)
        Iterator<EdgeWritable> rf_edges = n1.getEdgeList(DirectionFlag.DIR_RF).iterator();
        int k2_index = 0, k3_index = 0;
        for (int i=0; i < n1.getEdgeList(DirectionFlag.DIR_RF).getCountOfPosition(); i++) {
            VKmerBytesWritable curKmer = rf_edges.next().getKey();
            if (curKmer.equals(k2))
                k2_index = i;
            if (curKmer.equals(k3))
                k3_index = i;
        }
        
        Assert.assertEquals(2, n1.getEdgeList(DirectionFlag.DIR_RF).getCountOfPosition()); // only k2 and k3 in list
        Assert.assertEquals(k2, n1.getEdgeList(DirectionFlag.DIR_RF).get(k2_index).getKey());
        Assert.assertEquals(k3, n1.getEdgeList(DirectionFlag.DIR_RF).get(k3_index).getKey());
        Assert.assertEquals(3, n1.getEdgeList(DirectionFlag.DIR_RF).get(k2_index).getReadIDs().getCountOfPosition()); // k2 has r1,r2,r3
        Assert.assertEquals(1, n1.getEdgeList(DirectionFlag.DIR_RF).get(k3_index).getReadIDs().getCountOfPosition()); // k3 has r3 
        
        long[] k2_readids = n1.getEdgeList(DirectionFlag.DIR_RF).get(k2_index).readIDArray();
        Assert.assertEquals(3, k2_readids.length);
        long[] k3_readids = n1.getEdgeList(DirectionFlag.DIR_RF).get(k3_index).readIDArray();
        Assert.assertEquals(1, k3_readids.length);
        
        boolean[] found = {false, false, false};
        for (int i=0; i < k2_readids.length; i++) { // the readid list itself may also be out of order
            if (k2_readids[i] == read1.getReadId())
                found[0] = true;
            else if (k2_readids[i] == read2.getReadId())
                found[1] = true;
            else if (k2_readids[i] == read3.getReadId())
                found[2] = true;
        }
        Assert.assertTrue(found[0]);
        Assert.assertTrue(found[1]);
        Assert.assertTrue(found[2]);
        
        Assert.assertEquals(read3.getReadId(), k3_readids[0]);
    }
}
