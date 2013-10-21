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
import edu.uci.ics.genomix.type.EDGETYPE;

public class EdgeMapTest {

//    @Test
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
        n1.setAverageCoverage(10);
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
        n1.getEdgeMap(EDGETYPE.RF).put(k2, plist1);
        Assert.assertEquals(numelements / 3, n1.getEdgeMap(EDGETYPE.RF).get(k2).size());
        n1.getEdgeMap(EDGETYPE.RF).unionUpdate(new EdgeMap(Arrays.asList(new SimpleEntry<VKmer, ReadIdSet>(k2, plist2))));
        Assert.assertEquals(numelements * 2 / 3 + numoverlap, n1.getEdgeMap(EDGETYPE.RF).get(k2).size());
        n1.getEdgeMap(EDGETYPE.RF).unionUpdate(new EdgeMap(Arrays.asList(new SimpleEntry<VKmer, ReadIdSet>(k2, plist3))));
        Assert.assertEquals(numelements, n1.getEdgeMap(EDGETYPE.RF).get(k2).size());

        Long[] allReadIDs = n1.getEdgeMap(EDGETYPE.RF).get(k2).toArray(new Long[0]);
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

        @Test
        public void TestConstructor() throws IOException {
            String kmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
            VKmer kSample = new VKmer(kmerSample);
            SimpleEntry<VKmer, ReadIdSet> sample;
            ReadIdSet positionsSample = new ReadIdSet();
            long numelements = 89432;
            for (long i = 0; i < numelements; i++) {
                positionsSample.add(i);
            }
            sample = new SimpleEntry<VKmer, ReadIdSet>(kSample, positionsSample);
            ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
            sampleList.add(sample);
            EdgeMap toTest = new EdgeMap(sampleList);
            Assert.assertEquals(numelements, toTest.get(kSample).size());
            for(long i = 0; i < numelements; i++) {
                Assert.assertEquals((Long)i, toTest.get(kSample).pollFirst());
            }
        }
        
        @Test
        public void TestSetAsCopy() throws IOException {
            String oldkmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
            VKmer oldKSample = new VKmer(oldkmerSample);
            SimpleEntry<VKmer, ReadIdSet> sample;
            ReadIdSet positionsSample = new ReadIdSet();
            long numelements = 89432;
            for (long i = 0; i < numelements; i++) {
                positionsSample.add(i);
            }
            sample = new SimpleEntry<VKmer, ReadIdSet>(oldKSample, positionsSample);
            ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
            sampleList.add(sample);
            EdgeMap source = new EdgeMap(sampleList);
            //begin test
            EdgeMap target = new EdgeMap();
            target.setAsCopy(source);
            source.remove(oldKSample);
            Assert.assertEquals(oldkmerSample, target.firstKey().toString());
            //finish test
        }
                
        @Test
        public void TestgetEdge() throws IOException {
            String oldkmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
            VKmer oldKSample = new VKmer(oldkmerSample);
            SimpleEntry<VKmer, ReadIdSet> sample;
            ReadIdSet positionsSample = new ReadIdSet();
            long numelements = 89432;
            for (long i = 0; i < numelements; i++) {
                positionsSample.add(i);
            }
            sample = new SimpleEntry<VKmer, ReadIdSet>(oldKSample, positionsSample);
            ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
            sampleList.add(sample);
            EdgeMap source = new EdgeMap(sampleList);
            long number = 122;
            Assert.assertEquals((Long)number, source.get(oldKSample).floor((Long)(number)));
        }
        
        @Test
        public void TestByteStreamReadWrite() throws IOException {
            String oldkmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
            VKmer oldKSample = new VKmer(oldkmerSample);
            SimpleEntry<VKmer, ReadIdSet> sample;
            ReadIdSet positionsSample = new ReadIdSet();
            long numelements = 898852;
            for (long i = 0; i < numelements; i++) {
                positionsSample.add(i);
            }
            sample = new SimpleEntry<VKmer, ReadIdSet>(oldKSample, positionsSample);
            ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
            sampleList.add(sample);
            EdgeMap toTest = new EdgeMap(sampleList);
            //begin test
            ByteArrayOutputStream baos = new ByteArrayOutputStream(toTest.getLengthInBytes());
            DataOutputStream out = new DataOutputStream(baos);
            toTest.write(out);
            InputStream inputStream = new ByteArrayInputStream(baos.toByteArray());
            DataInputStream in = new DataInputStream(inputStream);
            EdgeMap toTest2 = new EdgeMap();
            toTest2.readFields(in);
            long oldReadId = 123;
            Assert.assertEquals((Long)oldReadId, toTest2.get(oldKSample).floor((Long)oldReadId));
        }
        
        @Test
        public void TestRemoveSubSet() throws IOException {
            String oldkmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
            VKmer oldKSample = new VKmer(oldkmerSample);
            SimpleEntry<VKmer, ReadIdSet> sample;
            ReadIdSet positionsSample = new ReadIdSet();
            long numelements = 898852;
            for (long i = 0; i < numelements; i++) {
                positionsSample.add(i);
            }
            sample = new SimpleEntry<VKmer, ReadIdSet>(oldKSample, positionsSample);
            ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
            sampleList.add(sample);
            EdgeMap toTest = new EdgeMap(sampleList);
            //begin test
            ReadIdSet positionsSample2 = new ReadIdSet();
            long removeElements = 99;
            for (long i = 0; i < removeElements; i++) {
                positionsSample2.add(i * i * 2);
            }
            sample.setValue(positionsSample2);
            toTest.removeReadIdSubset(oldKSample, sample.getValue());
            boolean flag = false;
            
            for(long i = 0 ; i < removeElements; i++) {
                if(toTest.get(oldKSample).pollFirst() == (Long)(i * i * 2)){
                    flag = true;
                    break;
                }
            }
            Assert.assertFalse(flag);
        }
        
        @Test
        public void TestUnionUpdate() throws IOException {
            String kmerSample = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
            VKmer KSample = new VKmer(kmerSample);
            SimpleEntry<VKmer, ReadIdSet> sample;
            ReadIdSet positionsSample = new ReadIdSet();
            long numelements = 100;
            for (long i = 0; i < numelements; i++) {
                positionsSample.add(i % 50);
            }
            sample = new SimpleEntry<VKmer, ReadIdSet>(KSample, positionsSample);
            SimpleEntry<VKmer, ReadIdSet> sample2;
            for (long i = 0; i < numelements; i++) {
                positionsSample.add(i % 30);
            }
            sample2  = new SimpleEntry<VKmer, ReadIdSet>(KSample, positionsSample);
            ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
            ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList2 = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
            sampleList.add(sample);
            sampleList2.add(sample2);
            EdgeMap toTest = new EdgeMap(sampleList);
            EdgeMap toTest2 = new EdgeMap(sampleList2);
            toTest.unionUpdate(toTest2);
            ReadIdSet targetSample = new ReadIdSet();
            numelements = 50;
            for (long i = 0; i < 50; i++) {
                targetSample.add(i);
            }
            SimpleEntry<VKmer, ReadIdSet> targetEdge;
            targetEdge = new SimpleEntry<VKmer, ReadIdSet>(KSample, targetSample);
            ArrayList<SimpleEntry<VKmer, ReadIdSet>> targetList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
            targetList.add(targetEdge);
            EdgeMap toTarget = new EdgeMap(targetList);
            Assert.assertEquals(true, toTarget.equals(toTest));
        }
}
