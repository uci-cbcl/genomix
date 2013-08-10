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
        Assert.assertEquals("(50-0_0)", n1.getEdgeList(DirectionFlag.DIR_RF).get(0).getReadIDs().getPosition(0).toString());
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
        Assert.assertEquals("(100-0_0)", n3.getEdgeList(DirectionFlag.DIR_RR).get(0).getReadIDs().getPosition(0).toString());
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
}
