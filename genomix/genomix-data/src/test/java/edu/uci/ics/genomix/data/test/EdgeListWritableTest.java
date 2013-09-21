package edu.uci.ics.genomix.data.test;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;

public class EdgeListWritableTest {

    @Test
    public void TestGraphBuildNodes() throws IOException {
        KmerBytesWritable.setGlobalKmerLength(55);
        String kmer1 = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
        String kmer2 = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";

        VKmerBytesWritable k1 = new VKmerBytesWritable(kmer1);
        VKmerBytesWritable k2 = new VKmerBytesWritable(kmer2);
        PositionListWritable plist1 = new PositionListWritable();
        PositionListWritable plist2 = new PositionListWritable();
        PositionListWritable plist3 = new PositionListWritable();
        NodeWritable n1 = new NodeWritable();
        n1.setInternalKmer(k1);
        n1.setAvgCoverage(10);
        long numelements = 100000;
        long numoverlap = numelements / 10;
        for (long i = 0; i < numelements / 3; i++) {
            plist1.appendReadId(i);
        }
        for (long i = numelements / 3 - numoverlap; i < numelements * 2 / 3 + numoverlap; i++) {
            plist2.appendReadId(i);
        }
        for (long i = numelements * 2 / 3; i < numelements; i++) {
            plist3.appendReadId(i);
        }
        n1.getEdgeList(EDGETYPE.FF).add(new EdgeWritable(k2, plist1));
        Assert.assertEquals(numelements / 3, n1.getEdgeList(EDGETYPE.RF).get(0).getReadIDs()
                .getCountOfPosition());
        n1.getEdgeList(EDGETYPE.RF).unionUpdate(
                new EdgeListWritable(Arrays.asList(new EdgeWritable(k2, plist2))));
        Assert.assertEquals(numelements * 2 / 3 + numoverlap, n1.getEdgeList(EDGETYPE.RF).get(0).getReadIDs()
                .getCountOfPosition());
        n1.getEdgeList(EDGETYPE.RF).unionUpdate(
                new EdgeListWritable(Arrays.asList(new EdgeWritable(k2, plist3))));
        Assert.assertEquals(numelements, n1.getEdgeList(EDGETYPE.RF).get(0).getReadIDs().getCountOfPosition());

        long[] allReadIDs = n1.getEdgeList(EDGETYPE.RF).get(0).readIDArray();
        // make sure all readids are accounted for...
        for (long i = 0; i < numelements; i++) {
            boolean found = false;
            for (int j = 0; j < numelements; j++) {
                if (i == allReadIDs[j]) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("Didn't find element " + i, found);
        }
    }
}
