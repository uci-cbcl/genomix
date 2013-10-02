package edu.uci.ics.genomix.data.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.ReadHeadSet;
import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.Node.EDGETYPE;

public class EdgeListWritableTest {

    @Test
    public void TestGraphBuildNodes() throws IOException {
        Kmer.setGlobalKmerLength(55);
        String kmer1 = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
        String kmer2 = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";

        VKmer k1 = new VKmer(kmer1);
        VKmer k2 = new VKmer(kmer2);
        ReadIdSet plist1 = new ReadIdSet();
        ReadIdSet plist2 = new ReadIdSet();
        ReadIdSet plist3 = new ReadIdSet();
        Node n1 = new Node();
        n1.setInternalKmer(k1);
        n1.setAvgCoverage(10);
        long numelements = 100000;
        long numoverlap = numelements / 10;
        for (long i = 0; i < numelements / 3; i++) {
            plist1.add(i);
        }
        for (long i = numelements / 3 - numoverlap; i < numelements * 2 / 3 + numoverlap; i++) {
            plist2.add(i);
        }
        for (long i = numelements * 2 / 3; i < numelements; i++) {
            plist3.add(i);
        }
        n1.getEdgeList(EDGETYPE.RF).put(k2, plist1);
        Assert.assertEquals(numelements / 3, n1.getEdgeList(EDGETYPE.RF).get(k2).size());
        n1.getEdgeList(EDGETYPE.RF).unionUpdate(new EdgeMap(Arrays.asList(new SimpleEntry<VKmer, ReadIdSet>(k2, plist2))));
        Assert.assertEquals(numelements * 2 / 3 + numoverlap, n1.getEdgeList(EDGETYPE.RF).get(k2).size());
        n1.getEdgeList(EDGETYPE.RF).unionUpdate(new EdgeMap(Arrays.asList(new SimpleEntry<VKmer, ReadIdSet>(k2, plist3))));
        Assert.assertEquals(numelements, n1.getEdgeList(EDGETYPE.RF).get(k2).size());

        Long[] allReadIDs = n1.getEdgeList(EDGETYPE.RF).get(k2).toArray(new Long[0]);
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
    
//    @Test
//    public void TestConstructor() throws IOException {
//        String kmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
//        VKmerBytesWritable kSample = new VKmerBytesWritable(kmerSample);
//        SimpleEntry<VKmerBytesWritable, ReadIdListWritable> sample;
//        ReadIdListWritable positionsSample = new ReadIdListWritable();
//        long numelements = 89432;
//        for (long i = 0; i < numelements; i++) {
//            positionsSample.add(i);
//        }
//        sample = new SimpleEntry<VKmerBytesWritable, ReadIdListWritable>(kSample, positionsSample);
//        ArrayList<SimpleEntry<VKmerBytesWritable, ReadIdListWritable>> sampleList = new ArrayList<SimpleEntry<VKmerBytesWritable, ReadIdListWritable>>();
//        sampleList.add(sample);
//        EdgeListWritable toTest = new EdgeListWritable(sampleList);
//        //begin test
//        Assert.assertEquals(numelements, toTest.get(0).getReadIDs().getCountOfPosition());
//        Assert.assertEquals(kmerSample, toTest.get(0).getKey().toString());
//        for(long i = 0; i < numelements; i++) {
//            Assert.assertEquals(i, toTest.get(0).getReadIDs().getPosition((int)i).getReadId());
//        }
//        //finish test
//    }
//    
//    @Test
//    public void TestSetAsCopy() throws IOException {
//        String oldkmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
//        VKmerBytesWritable oldKSample = new VKmerBytesWritable(oldkmerSample);
//        EdgeWritable sample = new EdgeWritable();
//        PositionListWritable positionsSample = new PositionListWritable();
//        long numelements = 89432;
//        for (long i = 0; i < numelements; i++) {
//            positionsSample.appendReadId(i);
//        }
//        sample.setAsCopy(oldKSample, positionsSample);
//        ArrayList<EdgeWritable> sampleList = new ArrayList<EdgeWritable>();
//        sampleList.add(sample);
//        EdgeListWritable source = new EdgeListWritable(sampleList);
//        //begin test
//        EdgeListWritable target = new EdgeListWritable();
//        target.setAsCopy(source);
//        String newKmerSample = "GCTAGACTAC";
//        VKmerBytesWritable newKSample = new VKmerBytesWritable(newKmerSample);
//        source.get(0).setKey(newKSample);
//        Assert.assertEquals(oldkmerSample, target.get(0).getKey().toString());
//        //finish test
//    }
//    
//    @Test
//    public void TestAdd() throws IOException {
//        String oldkmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
//        VKmerBytesWritable oldKSample = new VKmerBytesWritable(oldkmerSample);
//        EdgeWritable sample = new EdgeWritable();
//        PositionListWritable positionsSample = new PositionListWritable();
//        long numelements = 89432;
//        for (long i = 0; i < numelements; i++) {
//            positionsSample.appendReadId(i);
//        }
//        sample.setAsCopy(oldKSample, positionsSample);
//        EdgeListWritable toTest = new EdgeListWritable();
//        //begin test
//        toTest.add(sample);
//        sample.getReadIDs().resetPosition(100, 999999);
//        String oldReadId = "(100-0_0)";
//        Assert.assertEquals(oldReadId,toTest.get(0).getReadIDs().getPosition(100).toString());
//    }
//    
//    @Test
//    public void Testset() throws IOException {
//        String oldkmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
//        VKmerBytesWritable oldKSample = new VKmerBytesWritable(oldkmerSample);
//        EdgeWritable sample = new EdgeWritable();
//        PositionListWritable positionsSample = new PositionListWritable();
//        long numelements = 89432;
//        for (long i = 0; i < numelements; i++) {
//            positionsSample.appendReadId(i);
//        }
//        sample.setAsCopy(oldKSample, positionsSample);
//        EdgeListWritable toTest = new EdgeListWritable();
//        toTest.add(sample);
//        String newkmerSample = "ATGCATGCGCTACCCCCCCCTAGCTAGACTACG";
//        VKmerBytesWritable newSample = new VKmerBytesWritable(newkmerSample);
//        sample.setKey(newSample);
//        //begin test
//        toTest.setAsCopy(0, sample);
//        Assert.assertEquals(newkmerSample, toTest.get(0).getKey().toString());
//    }
//    
//    @Test
//    public void TestgetEdge() throws IOException {
//        String oldkmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
//        VKmerBytesWritable oldKSample = new VKmerBytesWritable(oldkmerSample);
//        EdgeWritable sample = new EdgeWritable();
//        PositionListWritable positionsSample = new PositionListWritable();
//        long numelements = 89432;
//        for (long i = 0; i < numelements; i++) {
//            positionsSample.appendReadId(i);
//        }
//        sample.setAsCopy(oldKSample, positionsSample);
//        EdgeListWritable toTest = new EdgeListWritable();
//        toTest.add(sample);
//        String oldReadId = "(100-0_0)";
//        Assert.assertEquals(oldReadId, toTest.getEdge(oldKSample).getReadIDs().getPosition(100).toString());
//    }
//    
//    @Test
//    public void TestByteStreamReadWrite() throws IOException {
//        String oldkmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
//        VKmerBytesWritable oldKSample = new VKmerBytesWritable(oldkmerSample);
//        EdgeWritable sample = new EdgeWritable();
//        PositionListWritable positionsSample = new PositionListWritable();
//        long numelements = 898852;
//        for (long i = 0; i < numelements; i++) {
//            positionsSample.appendReadId(i);
//        }
//        sample.setAsCopy(oldKSample, positionsSample);
//        ArrayList<EdgeWritable> sampleList = new ArrayList<EdgeWritable>();
//        sampleList.add(sample);
//        EdgeListWritable toTest = new EdgeListWritable(sampleList);
//        //begin test
//        ByteArrayOutputStream baos = new ByteArrayOutputStream(toTest.getLengthInBytes());
//        DataOutputStream out = new DataOutputStream(baos);
//        toTest.write(out);
//        InputStream inputStream = new ByteArrayInputStream(baos.toByteArray());
//        DataInputStream in = new DataInputStream(inputStream);
//        EdgeListWritable toTest2 = new EdgeListWritable();
//        toTest2.readFields(in);
//        String oldReadId = "(123-0_0)";
//        Assert.assertEquals(oldReadId, toTest2.getEdge(oldKSample).getReadIDs().getPosition(123).toString());
//    }
//    
//    @Test
//    public void TestRemoveSubEdge() throws IOException {
//        String oldkmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
//        VKmerBytesWritable oldKSample = new VKmerBytesWritable(oldkmerSample);
//        EdgeWritable sample = new EdgeWritable();
//        PositionListWritable positionsSample = new PositionListWritable();
//        long numelements = 898852;
//        for (long i = 0; i < numelements; i++) {
//            positionsSample.appendReadId(i);
//        }
//        sample.setAsCopy(oldKSample, positionsSample);
//        ArrayList<EdgeWritable> sampleList = new ArrayList<EdgeWritable>();
//        sampleList.add(sample);
//        EdgeListWritable toTest = new EdgeListWritable(sampleList);
//        //begin test
//        PositionListWritable positionsSample2 = new PositionListWritable();
//        long removeElements = 99;
//        for (long i = 0; i < removeElements; i++) {
//            positionsSample2.appendReadId(i * i * 2);
//        }
//        sample.setAsCopy(oldKSample, positionsSample2);
//        toTest.removeReadIdSubset(sample);
//        boolean flag = false;
//        
//        for(long i = 0 ; i < removeElements; i++) {
//            PositionWritable forCompare  = new PositionWritable((byte)0, i*i*2, 0);
//            if(toTest.get(0).getReadIDs().getPosition((int) i) == forCompare){
//                flag = true;
//                break;
//            }
//        }
//        Assert.assertFalse(flag);
//    }
//    
//    @Test
//    public void TestUnionUpdate() throws IOException {
//        String kmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
//        VKmerBytesWritable KSample = new VKmerBytesWritable(kmerSample);
//        EdgeWritable sample = new EdgeWritable();
//        PositionListWritable positionsSample = new PositionListWritable();
//        long numelements = 100;
//        for (long i = 0; i < numelements; i++) {
//            positionsSample.appendReadId(i % 50);
//        }
//        sample.setAsCopy(KSample, positionsSample);
//        EdgeWritable sample2 = new EdgeWritable();
//        for (long i = 0; i < numelements; i++) {
//            positionsSample.appendReadId(i % 30);
//        }
//        sample2.setAsCopy(KSample, positionsSample);
//        ArrayList<EdgeWritable> sampleList = new ArrayList<EdgeWritable>();
//        sampleList.add(sample);
//        ArrayList<EdgeWritable> sampleList2 = new ArrayList<EdgeWritable>();
//        sampleList2.add(sample2);
//        EdgeListWritable toTest = new EdgeListWritable(sampleList);
//        EdgeListWritable toTest2 = new EdgeListWritable(sampleList2);
//        toTest.unionUpdate(toTest2);
//        PositionListWritable targetSample = new PositionListWritable();
//        numelements = 50;
//        for (long i = 0; i < 50; i++) {
//            targetSample.appendReadId(i);
//        }
//        EdgeWritable targetEdge = new EdgeWritable();
//        targetEdge.setAsCopy(KSample, targetSample);
//        ArrayList<EdgeWritable> targeList = new ArrayList<EdgeWritable>();
//        targeList.add(targetEdge);
//        EdgeListWritable toTarget = new EdgeListWritable(targeList);
//        Assert.assertEquals(true, toTarget.equals(toTest));
//    }
}
