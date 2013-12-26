package edu.uci.ics.genomix.data.types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.data.types.Node.NeighborInfo;
import edu.uci.ics.genomix.data.types.Node.READHEAD_ORIENTATION;

public class NodeFixedTest {

    public static void getEdgeMapRandomly(EdgeMap edgeMap, int orderNum) {
        int min = 3;
        int max = 4;
        ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList;
        SimpleEntry<VKmer, ReadIdSet> edgeId;
        for (EDGETYPE e : EDGETYPE.values()) {
            sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
            for (int i = 0; i < min + (int) (Math.random() * ((max - min) + 1)); i++) {
                String edgeStr = generateString(orderNum);
                VKmer edgeKmer = new VKmer(edgeStr);
                ReadIdSet edgeIdSet = new ReadIdSet();
                for (long j = 0; j < min + (int) (Math.random() * ((max - min) + 1)); j++) {
                    edgeIdSet.add(j);
                }
                edgeId = new SimpleEntry<VKmer, ReadIdSet>(edgeKmer, edgeIdSet);
                sampleList.add(edgeId);
            }
            edgeMap = new EdgeMap(sampleList);
        }

    }

    public static void compareEdgeMap(EdgeMap et1, EdgeMap et2) {
        Assert.assertEquals(et1.size(), et2.size());
        for (Map.Entry<VKmer, ReadIdSet> iter1 : et1.entrySet()) {
            Map.Entry<VKmer, ReadIdSet> iter2 = et2.pollFirstEntry();
            Assert.assertEquals(iter1.getKey().toString(), iter2.getKey().toString());
            for (Long readidIter1 : iter1.getValue()) {
                Long readidIter2 = iter2.getValue().pollFirst();
                Assert.assertEquals(readidIter1.toString(), readidIter2.toString());
            }
        }
    }

    public static void getUnflippedReadIdsAndEndReadsRandomly(ReadHeadSet readSet, int orderNum) {
        int min = 3;
        int max = 5;
        for (int i = 0; i < min + (int) (Math.random() * ((max - min) + 1)); i++) {
            readSet.add((byte) 1, (long) orderNum + i, i);
        }
    }

    public static void compareStartReadsAndEndReads(ReadHeadSet et1, ReadHeadSet et2) {
        Assert.assertEquals(et1.size(), et2.size());
        for (ReadHeadInfo iter1 : et1) {
            ReadHeadInfo iter2 = et2.pollFirst();
            Assert.assertEquals(iter1.toString(), iter2.toString());
        }
    }

    /**
     * basic checking for enum DIR in Node class
     * 
     * @throws IOException
     */
    @Test
    public void testDIR() throws IOException {
        Assert.assertEquals(0b01 << 2, DIR.REVERSE.get());
        Assert.assertEquals(0b10 << 2, DIR.FORWARD.get());
        DIR testDir1 = DIR.FORWARD;
        DIR testDir2 = DIR.REVERSE;
        Assert.assertEquals(DIR.REVERSE, testDir1.mirror());
        Assert.assertEquals(DIR.FORWARD, testDir2.mirror());
        Assert.assertEquals(0b11 << 2, DIR.fromSet(EnumSet.allOf(DIR.class)));
        Assert.assertEquals(0b00 << 2, DIR.fromSet(EnumSet.noneOf(DIR.class)));

        EnumSet<EDGETYPE> edgeTypes1 = testDir1.edgeTypes();
        EnumSet<EDGETYPE> edgeExample1 = EnumSet.noneOf(EDGETYPE.class);
        EnumSet<EDGETYPE> edgeTypes2 = testDir2.edgeTypes();
        EnumSet<EDGETYPE> edgeExample2 = EnumSet.noneOf(EDGETYPE.class);
        edgeExample1.add(EDGETYPE.FF);
        edgeExample1.add(EDGETYPE.FR);
        Assert.assertEquals(edgeExample1, edgeTypes1);

        edgeExample2.add(EDGETYPE.RF);
        edgeExample2.add(EDGETYPE.RR);
        Assert.assertEquals(edgeExample2, edgeTypes2);

        Assert.assertEquals(edgeExample1, DIR.edgeTypesInDir(testDir1));
        Assert.assertEquals(edgeExample2, DIR.edgeTypesInDir(testDir2));

        EnumSet<DIR> dirExample = EnumSet.noneOf(DIR.class);
        dirExample.add(DIR.FORWARD);
        Assert.assertEquals(dirExample, DIR.enumSetFromByte((short) 8));
        dirExample.clear();
        dirExample.add(DIR.REVERSE);
        Assert.assertEquals(dirExample, DIR.enumSetFromByte((short) 4));

        dirExample.clear();
        dirExample.add(DIR.FORWARD);
        Assert.assertEquals(dirExample, DIR.flipSetFromByte((short) 4));
        dirExample.clear();
        dirExample.add(DIR.REVERSE);
        Assert.assertEquals(dirExample, DIR.flipSetFromByte((short) 8));
    }

    /**
     * basic checking for EDGETYPE in Node class
     * 
     * @throws IOException
     */
    @Test
    public void testEDGETYPE() throws IOException {
        //fromByte()
        Assert.assertEquals(EDGETYPE.FF, EDGETYPE.fromByte((byte) 0));
        Assert.assertEquals(EDGETYPE.FR, EDGETYPE.fromByte((byte) 1));
        Assert.assertEquals(EDGETYPE.RF, EDGETYPE.fromByte((byte) 2));
        Assert.assertEquals(EDGETYPE.RR, EDGETYPE.fromByte((byte) 3));
        //mirror()
        Assert.assertEquals(EDGETYPE.RR, EDGETYPE.FF.mirror());
        Assert.assertEquals(EDGETYPE.FR, EDGETYPE.FR.mirror());
        Assert.assertEquals(EDGETYPE.RF, EDGETYPE.RF.mirror());
        Assert.assertEquals(EDGETYPE.FF, EDGETYPE.RR.mirror());
        //DIR()
        Assert.assertEquals(DIR.FORWARD, EDGETYPE.FF.dir());
        Assert.assertEquals(DIR.FORWARD, EDGETYPE.FR.dir());
        Assert.assertEquals(DIR.REVERSE, EDGETYPE.RF.dir());
        Assert.assertEquals(DIR.REVERSE, EDGETYPE.RR.dir());
        //resolveEdgeThroughPath()
        Assert.assertEquals(EDGETYPE.RF,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 0), EDGETYPE.fromByte((byte) 2)));
        Assert.assertEquals(EDGETYPE.RR,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 0), EDGETYPE.fromByte((byte) 3)));

        Assert.assertEquals(EDGETYPE.FF,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 1), EDGETYPE.fromByte((byte) 2)));
        Assert.assertEquals(EDGETYPE.FR,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 1), EDGETYPE.fromByte((byte) 3)));

        Assert.assertEquals(EDGETYPE.RF,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 2), EDGETYPE.fromByte((byte) 0)));
        Assert.assertEquals(EDGETYPE.RR,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 2), EDGETYPE.fromByte((byte) 1)));

        Assert.assertEquals(EDGETYPE.FF,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 3), EDGETYPE.fromByte((byte) 0)));
        Assert.assertEquals(EDGETYPE.FR,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 3), EDGETYPE.fromByte((byte) 1)));
        //causeFlip()
        Assert.assertEquals(false, EDGETYPE.FF.causesFlip());
        Assert.assertEquals(true, EDGETYPE.FR.causesFlip());
        Assert.assertEquals(true, EDGETYPE.RF.causesFlip());
        Assert.assertEquals(false, EDGETYPE.RR.causesFlip());
        //flipNeighbor()
        Assert.assertEquals(true, EDGETYPE.sameOrientation(EDGETYPE.RF, EDGETYPE.FR));
        Assert.assertEquals(false, EDGETYPE.sameOrientation(EDGETYPE.RF, EDGETYPE.RR));
    }

    @Test
    public void testREADHEAD_ORIENTATION() throws IOException {
        Assert.assertEquals(READHEAD_ORIENTATION.FLIPPED, READHEAD_ORIENTATION.fromByte((byte) 1));
        Assert.assertEquals(READHEAD_ORIENTATION.UNFLIPPED, READHEAD_ORIENTATION.fromByte((byte) 0));
    }

    @Test
    public void testNeighborsInfo() throws IOException {
        String sample1Str = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATC";
        VKmer oldKSample = new VKmer(sample1Str);
        SimpleEntry<VKmer, ReadIdSet> sample;
        ReadIdSet positionsSample = new ReadIdSet();
        long numelements = 10;
        for (long i = 0; i < numelements; i++) {
            positionsSample.add(i);
        }
        sample = new SimpleEntry<VKmer, ReadIdSet>(oldKSample, positionsSample);
        ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
        sampleList.add(sample);

        String sample2Str = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGAT";
        VKmer oldKSample2 = new VKmer(sample2Str);
        SimpleEntry<VKmer, ReadIdSet> sample2;
        ReadIdSet positionsSample2 = new ReadIdSet();
        long numelements2 = 20;
        for (long i = 10; i < numelements2; i++) {
            positionsSample2.add(i);
        }
        sample2 = new SimpleEntry<VKmer, ReadIdSet>(oldKSample2, positionsSample2);
        sampleList.add(sample2);
        EdgeMap source = new EdgeMap(sampleList);
        Node.NeighborsInfo neighborsInfor = new Node.NeighborsInfo(EDGETYPE.FF, source);
        Iterator<NeighborInfo> iterator = neighborsInfor.iterator();
        long i = 0;
        Assert.assertEquals(true, iterator.hasNext());
        NeighborInfo temp = iterator.next();
        Assert.assertEquals(EDGETYPE.FF, temp.et);
        //        System.out.println(temp.kmer.toString());
        Assert.assertEquals(sample1Str, temp.kmer.toString());
        for (; i < numelements; i++) {
            //            System.out.println(temp.readIds.pollFirst().toString());
            Assert.assertEquals((Long) i, temp.readIds.pollFirst());
        }
    }

    @Test
    public void testNodeReset() throws IOException {
        String internalStr = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATC";
        VKmer internalSample = new VKmer(internalStr);
        String sampleStr = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATC";
        VKmer oldKSample = new VKmer(sampleStr);
        SimpleEntry<VKmer, ReadIdSet> sample;
        ReadIdSet positionsSample = new ReadIdSet();
        long numelements = 10;
        for (long i = 0; i < numelements; i++) {
            positionsSample.add(i);
        }
        sample = new SimpleEntry<VKmer, ReadIdSet>(oldKSample, positionsSample);
        ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
        sampleList.add(sample);
        EdgeMap edge = new EdgeMap(sampleList);
        //-------------------------------------------
        ReadHeadSet startReads = new ReadHeadSet();
        ReadHeadSet endReads = new ReadHeadSet();
        byte mateId;
        long readId;
        int posId;
        for (int i = 0; i < 5; i++) {
            mateId = (byte) 1;
            readId = (long) i;
            posId = i;
            startReads.add(mateId, readId, posId);
            Assert.assertEquals(i + 1, startReads.size());
        }
        for (int i = 5; i < 10; i++) {
            mateId = (byte) 0;
            readId = (long) i;
            posId = i;
            endReads.add(mateId, readId, posId);
            Assert.assertEquals(i - 5 + 1, endReads.size());
        }
        Node node = new Node();
        node.setInternalKmer(internalSample);
        node.setEdgeMap(EDGETYPE.RF, edge);
        node.setAverageCoverage((float) 54.6);
        node.setUnflippedReadIds(startReads);
        node.setFlippedReadIds(endReads);
        node.reset();
        Assert.assertEquals((float) 0, node.getAverageCoverage());
        Assert.assertEquals(true, node.getEdgeMap(EDGETYPE.RF).isEmpty());
        Assert.assertEquals(4, node.getInternalKmer().getLength()); //only left the bytes which contain the header
        Assert.assertEquals(true, node.getUnflippedReadIds().isEmpty());
        Assert.assertEquals(true, node.getFlippedReadIds().isEmpty());
    }

    @Test
    public void testSetCopyWithNode() throws IOException {
        Node srcNode = new Node();
        NodeFixedTest.assembleNodeRandomly(srcNode, 10);
        Node targetNode = new Node();
        targetNode.setAsCopy(srcNode);
        NodeFixedTest.compareTwoNodes(srcNode, targetNode);
    }

    @Test
    public void testSetCopyAndRefWithByteArray() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Node[] dataNodes = new Node[5];
        for (int i = 0; i < 5; i++)
            dataNodes[i] = new Node();
        int[] nodeOffset = new int[5];

        for (int i = 10; i < 15; i++) {
            NodeFixedTest.assembleNodeRandomly(dataNodes[i - 10], i);
            nodeOffset[i - 10] = dataNodes[i - 10].getSerializedLength();
            outputStream.write(dataNodes[i - 10].marshalToByteArray());
        }
        byte[] dataArray = outputStream.toByteArray();
        Node testCopyNode = new Node();
        for (int i = 0; i < 5; i++) {
            int totalOffset = 0;
            for (int j = 0; j < i; j++) {
                totalOffset += nodeOffset[j];
            }
            testCopyNode.setAsCopy(dataArray, totalOffset);
            NodeFixedTest.compareTwoNodes(dataNodes[i], testCopyNode);
        }
        Node testRefNode = new Node();
        for (int i = 0; i < 5; i++) {
            int totalOffset = 0;
            for (int j = 0; j < i; j++) {
                totalOffset += nodeOffset[j];
            }
            testRefNode.setAsReference(dataArray, totalOffset);
            NodeFixedTest.compareTwoNodes(dataNodes[i], testRefNode);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNeighborEdgeTypeWithException() {
        Node testNode = new Node();
        NodeFixedTest.assembleNodeRandomly(testNode, 20);
        testNode.getNeighborEdgeType(DIR.FORWARD);
    }

    @Test
    public void testGetNeighborEdgeType() {
        Node testNode = new Node();
        NodeFixedTest.assembleNodeRandomly(testNode, 20);
        testNode.getEdgeMap(EDGETYPE.FF).clear();
        testNode.getEdgeMap(EDGETYPE.FR).clear();
        testNode.getEdgeMap(EDGETYPE.RF).clear();
        int totalCount = testNode.getEdgeMap(EDGETYPE.RR).size();
        for (int i = 0; i < totalCount - 1; i++) {
            testNode.getEdgeMap(EDGETYPE.RR).pollFirstEntry();
        }
        Assert.assertEquals(EDGETYPE.RR, testNode.getNeighborEdgeType(DIR.REVERSE));
    }

    @Test
    public void testGetSingleNeighbor() {
        Node testNode = new Node();
        NodeFixedTest.assembleNodeRandomly(testNode, 20);
        Assert.assertEquals(null, testNode.getSingleNeighbor(DIR.FORWARD));
    }

    @Test
    public void testSetEdgeMap() {
        Node testNode = new Node();
        NodeFixedTest.assembleNodeRandomly(testNode, 20);
        EdgeMap[] edge = new EdgeMap[4];
        for (int i = 0; i < 4; i++) {
            edge[i] = new EdgeMap();
        }
        for (int i = 0; i < 4; i++) {
            getEdgeMapRandomly(edge[i], 10 + i);
        }

        testNode.setEdgeMap(EDGETYPE.FF, edge[0]);
        testNode.setEdgeMap(EDGETYPE.FR, edge[1]);
        testNode.setEdgeMap(EDGETYPE.RF, edge[2]);
        testNode.setEdgeMap(EDGETYPE.RR, edge[3]);
        NodeFixedTest.compareEdgeMap(testNode.getEdgeMap(EDGETYPE.FF), edge[0]);
        NodeFixedTest.compareEdgeMap(testNode.getEdgeMap(EDGETYPE.FR), edge[1]);
        NodeFixedTest.compareEdgeMap(testNode.getEdgeMap(EDGETYPE.RF), edge[2]);
        NodeFixedTest.compareEdgeMap(testNode.getEdgeMap(EDGETYPE.RR), edge[3]);
    }

    @Test
    public void testMergeCoverage() {
        Node testNode1 = new Node();
        NodeFixedTest.assembleNodeRandomly(testNode1, 27);
        Kmer fixedKmer = new Kmer();
        fixedKmer.setGlobalKmerLength(13);
        Node testNode2 = new Node();
        NodeFixedTest.assembleNodeRandomly(testNode2, 32);
        //get mergeCoverage manually first
        float adjustedLength = testNode1.getKmerLength() + testNode2.getKmerLength() - (Kmer.getKmerLength() - 1) * 2;
        float node1Count = (testNode1.getKmerLength() - (Kmer.getKmerLength() - 1)) * testNode1.getAverageCoverage();
        float node2Count = (testNode2.getKmerLength() - (Kmer.getKmerLength() - 1)) * testNode2.getAverageCoverage();
        float expectedCoverage = (node1Count + node2Count) / adjustedLength;
        testNode1.mergeCoverage(testNode2);
        Assert.assertEquals(expectedCoverage, testNode1.getAverageCoverage());
    }

    @Test
    public void testAddCoverage() {
        Node testNode1 = new Node();
        NodeFixedTest.assembleNodeRandomly(testNode1, 27);
        Kmer fixedKmer = new Kmer();
        fixedKmer.setGlobalKmerLength(13);
        Node testNode2 = new Node();
        NodeFixedTest.assembleNodeRandomly(testNode2, 32);
        //get mergeCoverage manually first
        float node1adjustedLength = testNode1.getKmerLength() - Kmer.getKmerLength() + 1;
        float node2adjustedLength = testNode2.getKmerLength() - Kmer.getKmerLength() + 1;
        float node1AverageCoverage = testNode1.getAverageCoverage() + testNode2.getAverageCoverage()
                * (node2adjustedLength) / node1adjustedLength;
        testNode1.addCoverage(testNode2);
        Assert.assertEquals(node1AverageCoverage, testNode1.getAverageCoverage());
    }

    @Test
    public void testSeartReadsAndEndReads() {
        ReadHeadSet[] startAndEndArray = new ReadHeadSet[2];
        for (int i = 0; i < 2; i++)
            startAndEndArray[i] = new ReadHeadSet();
        NodeFixedTest.getUnflippedReadIdsAndEndReadsRandomly(startAndEndArray[0], 17);
        NodeFixedTest.getUnflippedReadIdsAndEndReadsRandomly(startAndEndArray[1], 26);
        Node testNode = new Node();
        NodeFixedTest.assembleNodeRandomly(testNode, 35);
        testNode.setUnflippedReadIds(startAndEndArray[0]);
        testNode.setFlippedReadIds(startAndEndArray[1]);
        NodeFixedTest.compareStartReadsAndEndReads(startAndEndArray[0], testNode.getUnflippedReadIds());
        NodeFixedTest.compareStartReadsAndEndReads(startAndEndArray[1], testNode.getFlippedReadIds());
    }

    @Test
    public void testWriteAndReadFields() throws IOException {
        Node srcNode = new Node();
        NodeFixedTest.assembleNodeRandomly(srcNode, 17);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(srcNode.getSerializedLength());
        DataOutputStream out = new DataOutputStream(baos);
        srcNode.write(out);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(inputStream);
        Node testNode = new Node();
        testNode.readFields(in);
        NodeFixedTest.compareTwoNodes(srcNode, testNode);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeEdgeWithFFException() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 13);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 16);
        majorNode.mergeEdges(EDGETYPE.FF, minorNode);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeEdgeWithFRException() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 13);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 16);
        majorNode.mergeEdges(EDGETYPE.FF, minorNode);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeEdgeWithRFException() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 13);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 16);
        majorNode.mergeEdges(EDGETYPE.FF, minorNode);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeEdgeWithRRException() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 13);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 16);
        majorNode.mergeEdges(EDGETYPE.FF, minorNode);
    }

    @Test
    public void testMergeEdgeWithFF() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 16);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 18);
        majorNode.getEdgeMap(EDGETYPE.FF).clear();
        majorNode.getEdgeMap(EDGETYPE.FR).clear();
        minorNode.getEdgeMap(EDGETYPE.RF).clear();
        minorNode.getEdgeMap(EDGETYPE.RR).clear();
        majorNode.mergeEdges(EDGETYPE.FF, minorNode);
        NodeFixedTest.compareEdgeMap(majorNode.getEdgeMap(EDGETYPE.FF), minorNode.getEdgeMap(EDGETYPE.FF));
        NodeFixedTest.compareEdgeMap(majorNode.getEdgeMap(EDGETYPE.FR), minorNode.getEdgeMap(EDGETYPE.FR));
    }

    @Test
    public void testMergeEdgeWithFR() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 17);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 19);
        majorNode.getEdgeMap(EDGETYPE.FF).clear();
        majorNode.getEdgeMap(EDGETYPE.FR).clear();

        minorNode.getEdgeMap(EDGETYPE.FF).clear();
        minorNode.getEdgeMap(EDGETYPE.FR).clear();

        majorNode.mergeEdges(EDGETYPE.FR, minorNode);
        NodeFixedTest.compareEdgeMap(majorNode.getEdgeMap(EDGETYPE.FF), minorNode.getEdgeMap(EDGETYPE.RF));
        NodeFixedTest.compareEdgeMap(majorNode.getEdgeMap(EDGETYPE.FR), minorNode.getEdgeMap(EDGETYPE.RR));
    }

    @Test
    public void testMergeEdgeWithRF() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 17);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 19);
        majorNode.getEdgeMap(EDGETYPE.RF).clear();
        majorNode.getEdgeMap(EDGETYPE.RR).clear();

        minorNode.getEdgeMap(EDGETYPE.RF).clear();
        minorNode.getEdgeMap(EDGETYPE.RR).clear();

        majorNode.mergeEdges(EDGETYPE.RF, minorNode);
        NodeFixedTest.compareEdgeMap(majorNode.getEdgeMap(EDGETYPE.RF), minorNode.getEdgeMap(EDGETYPE.FF));
        NodeFixedTest.compareEdgeMap(majorNode.getEdgeMap(EDGETYPE.RR), minorNode.getEdgeMap(EDGETYPE.FR));
    }

    @Test
    public void testMergeEdgeWithRR() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 17);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 19);
        majorNode.getEdgeMap(EDGETYPE.RR).clear();
        majorNode.getEdgeMap(EDGETYPE.RF).clear();

        minorNode.getEdgeMap(EDGETYPE.FF).clear();
        minorNode.getEdgeMap(EDGETYPE.FR).clear();

        majorNode.mergeEdges(EDGETYPE.RR, minorNode);
        NodeFixedTest.compareEdgeMap(majorNode.getEdgeMap(EDGETYPE.RF), minorNode.getEdgeMap(EDGETYPE.RF));
        NodeFixedTest.compareEdgeMap(majorNode.getEdgeMap(EDGETYPE.RR), minorNode.getEdgeMap(EDGETYPE.RR));
    }

    @Test
    public void testMergeStartAndEndReadIDsWithFF() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
        Kmer fixedKmer = new Kmer();
        fixedKmer.setGlobalKmerLength(13);
        ReadHeadSet expectedStartReads = new ReadHeadSet(majorNode.getUnflippedReadIds());
        ReadHeadSet expectedEndReads = new ReadHeadSet(majorNode.getFlippedReadIds());
        int newOtherOffset = majorNode.getKmerLength() - fixedKmer.getKmerLength() + 1;
        for (ReadHeadInfo p : minorNode.getUnflippedReadIds()) {
            expectedStartReads.add(p.getMateId(), p.getReadId(), newOtherOffset + p.getOffset());
        }
        for (ReadHeadInfo p : minorNode.getFlippedReadIds()) {
            expectedEndReads.add(p.getMateId(), p.getReadId(), newOtherOffset + p.getOffset());
        }
        majorNode.mergeUnflippedAndFlippedReadIDs(EDGETYPE.FF, minorNode);
        NodeFixedTest.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
        NodeFixedTest.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
    }

    @Test
    public void testMergeStartAndEndReadIDsWithFR() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
        Kmer fixedKmer = new Kmer();
        fixedKmer.setGlobalKmerLength(13);
        ReadHeadSet expectedStartReads = new ReadHeadSet(majorNode.getUnflippedReadIds());
        ReadHeadSet expectedEndReads = new ReadHeadSet(majorNode.getFlippedReadIds());
        int newOtherOffset = majorNode.getKmerLength() - fixedKmer.getKmerLength() + minorNode.getKmerLength();
        for (ReadHeadInfo p : minorNode.getFlippedReadIds()) {
            expectedStartReads.add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset());
        }
        for (ReadHeadInfo p : minorNode.getUnflippedReadIds()) {
            expectedEndReads.add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset());
        }
        majorNode.mergeUnflippedAndFlippedReadIDs(EDGETYPE.FR, minorNode);
        NodeFixedTest.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
        NodeFixedTest.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
    }

    @Test
    public void testMergeStartAndEndReadIDsWithRF() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
        Kmer fixedKmer = new Kmer();
        fixedKmer.setGlobalKmerLength(13);
        ReadHeadSet expectedStartReads = new ReadHeadSet();
        ReadHeadSet expectedEndReads = new ReadHeadSet();
        int newThisOffset = minorNode.getKmerLength() - fixedKmer.getKmerLength() + 1;
        int newOtherOffset = minorNode.getKmerLength() - 1;
        for (ReadHeadInfo p : majorNode.getUnflippedReadIds()) {
            expectedStartReads.add(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
        }
        for (ReadHeadInfo p : majorNode.getFlippedReadIds()) {
            expectedEndReads.add(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
        }
        for (ReadHeadInfo p : minorNode.getUnflippedReadIds()) {
            expectedEndReads.add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset());
        }
        for (ReadHeadInfo p : minorNode.getFlippedReadIds()) {
            expectedStartReads.add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset());
        }
        majorNode.mergeUnflippedAndFlippedReadIDs(EDGETYPE.RF, minorNode);
        NodeFixedTest.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
        NodeFixedTest.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
    }

    @Test
    public void testMergeStartAndEndReadIDsWithRR() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
        Kmer fixedKmer = new Kmer();
        fixedKmer.setGlobalKmerLength(13);

        ReadHeadSet expectedStartReads = new ReadHeadSet();
        ReadHeadSet expectedEndReads = new ReadHeadSet();
        int newThisOffset = minorNode.getKmerLength() - fixedKmer.getKmerLength() + 1;
        for (ReadHeadInfo p : majorNode.getUnflippedReadIds()) {
            expectedStartReads.add(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
        }
        for (ReadHeadInfo p : majorNode.getFlippedReadIds()) {
            expectedEndReads.add(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
        }
        for (ReadHeadInfo p : minorNode.getUnflippedReadIds()) {
            expectedStartReads.add(p.getMateId(), p.getReadId(), p.getOffset());
        }
        for (ReadHeadInfo p : minorNode.getFlippedReadIds()) {
            expectedEndReads.add(p.getMateId(), p.getReadId(), p.getOffset());
        }
        majorNode.mergeUnflippedAndFlippedReadIDs(EDGETYPE.RR, minorNode);
        NodeFixedTest.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
        NodeFixedTest.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
    }

    @Test
    public void testAddEdgesWithNoFlips() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
        EdgeMap expectedFF = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.FF));
        EdgeMap expectedFR = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.FR));
        EdgeMap expectedRF = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.RF));
        EdgeMap expectedRR = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.RR));
        expectedFF.unionUpdate(minorNode.getEdgeMap(EDGETYPE.FF));
        expectedFR.unionUpdate(minorNode.getEdgeMap(EDGETYPE.FR));
        expectedRF.unionUpdate(minorNode.getEdgeMap(EDGETYPE.RF));
        expectedRR.unionUpdate(minorNode.getEdgeMap(EDGETYPE.RR));
        majorNode.addEdges(false, minorNode);
        NodeFixedTest.compareEdgeMap(expectedFF, majorNode.getEdgeMap(EDGETYPE.FF));
        NodeFixedTest.compareEdgeMap(expectedFR, majorNode.getEdgeMap(EDGETYPE.FR));
        NodeFixedTest.compareEdgeMap(expectedRF, majorNode.getEdgeMap(EDGETYPE.RF));
        NodeFixedTest.compareEdgeMap(expectedRR, majorNode.getEdgeMap(EDGETYPE.RR));
    }

    @Test
    public void testAddEdgesWithFlips() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 20);

        EdgeMap expectedFF = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.FF));
        EdgeMap expectedFR = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.FR));
        EdgeMap expectedRF = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.RF));
        EdgeMap expectedRR = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.RR));
        expectedFF.unionUpdate(minorNode.getEdgeMap(EDGETYPE.RF));
        expectedFR.unionUpdate(minorNode.getEdgeMap(EDGETYPE.RR));
        expectedRF.unionUpdate(minorNode.getEdgeMap(EDGETYPE.FF));
        expectedRR.unionUpdate(minorNode.getEdgeMap(EDGETYPE.FR));
        majorNode.addEdges(true, minorNode);
        NodeFixedTest.compareEdgeMap(expectedFF, majorNode.getEdgeMap(EDGETYPE.FF));
        NodeFixedTest.compareEdgeMap(expectedFR, majorNode.getEdgeMap(EDGETYPE.FR));
        NodeFixedTest.compareEdgeMap(expectedRF, majorNode.getEdgeMap(EDGETYPE.RF));
        NodeFixedTest.compareEdgeMap(expectedRR, majorNode.getEdgeMap(EDGETYPE.RR));
    }

    @Test
    public void testAddStartAndEndWithNoFlip() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
        Kmer fixedKmer = new Kmer();
        fixedKmer.setGlobalKmerLength(13);

        ReadHeadSet expectedStartReads = new ReadHeadSet(majorNode.getUnflippedReadIds());
        ReadHeadSet expectedEndReads = new ReadHeadSet(majorNode.getFlippedReadIds());
        float lengthFactor = (float) majorNode.getInternalKmer().getKmerLetterLength()
                / (float) minorNode.getInternalKmer().getKmerLetterLength();
        for (ReadHeadInfo p : minorNode.getUnflippedReadIds()) {
            expectedStartReads.add(p.getMateId(), p.getReadId(), (int) (p.getOffset() * lengthFactor));
        }
        for (ReadHeadInfo p : minorNode.getFlippedReadIds()) {
            expectedEndReads.add(p.getMateId(), p.getReadId(), (int) (p.getOffset() * lengthFactor));
        }
        majorNode.addUnflippedAndFlippedReadIds(false, minorNode);
        NodeFixedTest.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
        NodeFixedTest.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
    }

    @Test
    public void testAddStartAndEndWithFlip() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
        Kmer fixedKmer = new Kmer();
        fixedKmer.setGlobalKmerLength(13);

        ReadHeadSet expectedStartReads = new ReadHeadSet(majorNode.getUnflippedReadIds());
        ReadHeadSet expectedEndReads = new ReadHeadSet(majorNode.getFlippedReadIds());
        float lengthFactor = (float) majorNode.getInternalKmer().getKmerLetterLength()
                / (float) minorNode.getInternalKmer().getKmerLetterLength();
        int newPOffset;
        for (ReadHeadInfo p : minorNode.getUnflippedReadIds()) {
            newPOffset = minorNode.getInternalKmer().getKmerLetterLength() - 1 - p.getOffset();
            expectedEndReads.add(p.getMateId(), p.getReadId(), (int) (newPOffset * lengthFactor));
        }
        for (ReadHeadInfo p : minorNode.getFlippedReadIds()) {
            newPOffset = minorNode.getInternalKmer().getKmerLetterLength() - 1 - p.getOffset();
            expectedStartReads.add(p.getMateId(), p.getReadId(), (int) (newPOffset * lengthFactor));
        }
        majorNode.addUnflippedAndFlippedReadIds(true, minorNode);
        NodeFixedTest.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
        NodeFixedTest.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
    }

    @Test
    public void testUpdateEdges() {
        Node majorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
        Node minorNode = new Node();
        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
        Kmer fixedKmer = new Kmer();
        fixedKmer.setGlobalKmerLength(13);
        int ffEdgeCount = majorNode.getEdgeMap(EDGETYPE.FF).size() / 2;
        ArrayList<Map.Entry<VKmer, ReadIdSet>> iterFFList = new ArrayList<Map.Entry<VKmer, ReadIdSet>>();
        iterFFList.addAll(majorNode.getEdgeMap(EDGETYPE.FF).entrySet());

        int frEdgeCount = majorNode.getEdgeMap(EDGETYPE.FR).size() / 2;
        ArrayList<Map.Entry<VKmer, ReadIdSet>> iterFRList = new ArrayList<Map.Entry<VKmer, ReadIdSet>>();
        iterFRList.addAll(majorNode.getEdgeMap(EDGETYPE.FR).entrySet());

        int rfEdgeCount = majorNode.getEdgeMap(EDGETYPE.RF).size() / 2;
        ArrayList<Map.Entry<VKmer, ReadIdSet>> iterRFList = new ArrayList<Map.Entry<VKmer, ReadIdSet>>();
        iterRFList.addAll(majorNode.getEdgeMap(EDGETYPE.RF).entrySet());

        int rrEdgeCount = majorNode.getEdgeMap(EDGETYPE.RR).size() / 2;
        ArrayList<Map.Entry<VKmer, ReadIdSet>> iterRRList = new ArrayList<Map.Entry<VKmer, ReadIdSet>>();
        iterRRList.addAll(majorNode.getEdgeMap(EDGETYPE.RR).entrySet());

        EdgeMap expectedFF = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.FF));
        EdgeMap expectedFR = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.FR));
        EdgeMap expectedRF = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.RF));
        EdgeMap expectedRR = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.RR));

        expectedFF.remove(iterFFList.get(ffEdgeCount).getKey());
        expectedFF.unionUpdate(minorNode.getEdgeMap(EDGETYPE.FF));

        expectedFR.remove(iterFRList.get(frEdgeCount).getKey());
        expectedFR.unionUpdate(minorNode.getEdgeMap(EDGETYPE.FR));

        expectedRF.remove(iterRFList.get(rfEdgeCount).getKey());
        expectedRF.unionUpdate(minorNode.getEdgeMap(EDGETYPE.RF));

        expectedRR.remove(iterRRList.get(rrEdgeCount).getKey());
        expectedRR.unionUpdate(minorNode.getEdgeMap(EDGETYPE.RR));

        majorNode.updateEdges(EDGETYPE.FF, iterFFList.get(ffEdgeCount).getKey(), EDGETYPE.FF, EDGETYPE.FF, minorNode,
                true);
        majorNode.updateEdges(EDGETYPE.FR, iterFRList.get(frEdgeCount).getKey(), EDGETYPE.FR, EDGETYPE.FR, minorNode,
                true);
        majorNode.updateEdges(EDGETYPE.RF, iterRFList.get(rfEdgeCount).getKey(), EDGETYPE.RF, EDGETYPE.RF, minorNode,
                true);
        majorNode.updateEdges(EDGETYPE.RR, iterRRList.get(rrEdgeCount).getKey(), EDGETYPE.RR, EDGETYPE.RR, minorNode,
                true);
        NodeFixedTest.compareEdgeMap(expectedFF, majorNode.getEdgeMap(EDGETYPE.FF));
        NodeFixedTest.compareEdgeMap(expectedFR, majorNode.getEdgeMap(EDGETYPE.FR));
        NodeFixedTest.compareEdgeMap(expectedRF, majorNode.getEdgeMap(EDGETYPE.RF));
        NodeFixedTest.compareEdgeMap(expectedRR, majorNode.getEdgeMap(EDGETYPE.RR));
    }

    @Test
    public void testDegree() {
        Node node1 = new Node();
        NodeFixedTest.assembleNodeRandomly(node1, 20);
        Node node2 = new Node();
        NodeFixedTest.assembleNodeRandomly(node2, 21);
        Node node3 = new Node();
        NodeFixedTest.assembleNodeRandomly(node3, 22);
        Node node4 = new Node();
        NodeFixedTest.assembleNodeRandomly(node4, 23);

        Assert.assertEquals(node1.getEdgeMap(EDGETYPE.FF).size() + node1.getEdgeMap(EDGETYPE.FR).size(),
                node1.degree(DIR.FORWARD));
        Assert.assertEquals(node1.getEdgeMap(EDGETYPE.FF).size() + node1.getEdgeMap(EDGETYPE.FR).size(),
                node1.degree(DIR.FORWARD));
        Assert.assertEquals(node1.getEdgeMap(EDGETYPE.RF).size() + node1.getEdgeMap(EDGETYPE.RR).size(),
                node1.degree(DIR.REVERSE));
        Assert.assertEquals(node1.getEdgeMap(EDGETYPE.RF).size() + node1.getEdgeMap(EDGETYPE.RR).size(),
                node1.degree(DIR.REVERSE));
    }

    @Test
    public void testInAndOutdegree() {
        Node node = new Node();
        NodeFixedTest.assembleNodeRandomly(node, 20);
        Assert.assertEquals(node.getEdgeMap(EDGETYPE.FF).size() + node.getEdgeMap(EDGETYPE.FR).size(), node.outDegree());
        Assert.assertEquals(node.getEdgeMap(EDGETYPE.RF).size() + node.getEdgeMap(EDGETYPE.RR).size(), node.inDegree());
    }

    @Test
    public void testIsPathNode() {
        Node node = new Node();
        NodeFixedTest.assembleNodeRandomly(node, 20);
        Assert.assertEquals(false, node.isPathNode());
        node.getEdgeMap(EDGETYPE.FR).clear();
        node.getEdgeMap(EDGETYPE.RF).clear();
        int totalSize2 = node.getEdgeMap(EDGETYPE.FF).size();
        for (int i = 0; i < totalSize2 - 1; i++)
            node.getEdgeMap(EDGETYPE.FF).pollFirstEntry();

        int totalSize = node.getEdgeMap(EDGETYPE.RR).size();
        for (int i = 0; i < totalSize - 1; i++)
            node.getEdgeMap(EDGETYPE.RR).pollFirstEntry();
        Assert.assertEquals(true, node.isPathNode());
    }

    @Test
    public void testIsSimpleOrTerminalPath() {
        Node node = new Node();
        NodeFixedTest.assembleNodeRandomly(node, 20);
        Assert.assertEquals(false, node.isPathNode());
        node.getEdgeMap(EDGETYPE.FR).clear();
        node.getEdgeMap(EDGETYPE.RF).clear();
        node.getEdgeMap(EDGETYPE.RR).clear();
        int totalSize2 = node.getEdgeMap(EDGETYPE.FF).size();
        for (int i = 0; i < totalSize2 - 1; i++)
            node.getEdgeMap(EDGETYPE.FF).pollFirstEntry();
        Assert.assertEquals(true, node.isSimpleOrTerminalPath());

        Node node2 = new Node();
        NodeFixedTest.assembleNodeRandomly(node, 20);
        Assert.assertEquals(false, node.isPathNode());
        node.getEdgeMap(EDGETYPE.FR).clear();
        node.getEdgeMap(EDGETYPE.FF).clear();
        node.getEdgeMap(EDGETYPE.RR).clear();
        int totalSize1 = node.getEdgeMap(EDGETYPE.RF).size();
        for (int i = 0; i < totalSize1 - 1; i++)
            node.getEdgeMap(EDGETYPE.RF).pollFirstEntry();
        Assert.assertEquals(true, node.isSimpleOrTerminalPath());
    }
}
