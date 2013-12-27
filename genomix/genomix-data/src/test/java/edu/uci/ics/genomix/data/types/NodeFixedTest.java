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

    @Test
    public void testNeighborsInfo() throws IOException {
        String str1 = "ATGCATGCGCTAG";
        VKmer vkmer1 = new VKmer(str1);
        String str2 = "ATGCATCCCCTAG";
        VKmer vkmer2 = new VKmer(str1);
        String str3 = "AGGGATGCGCTAG";
        VKmer vkmer3 = new VKmer(str1);
        String str4 = "ATGCATAAATAC";
        VKmer vkmer4 = new VKmer(str1);
        VKmerList source = new VKmerList();
        source.append(vkmer1);
        source.append(vkmer2);
        source.append(vkmer3);
        source.append(vkmer4);
        Node.NeighborsInfo neighborsInfor = new Node.NeighborsInfo(EDGETYPE.FF, source);
        Iterator<NeighborInfo> iterator = neighborsInfor.iterator();
        NeighborInfo temp = iterator.next();
        Assert.assertEquals(EDGETYPE.FF, temp.et);
        Assert.assertEquals(str1, temp.kmer.toString());
        temp = iterator.next();
        Assert.assertEquals(str2, temp.kmer.toString());
        temp = iterator.next();
        Assert.assertEquals(str3, temp.kmer.toString());
        temp = iterator.next();
        Assert.assertEquals(str4, temp.kmer.toString());
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

        EDGETYPE[] edgeTypes1 = testDir1.edgeTypes();
        EnumSet<EDGETYPE> edgeExample1 = EnumSet.noneOf(EDGETYPE.class);
        EDGETYPE[] edgeTypes2 = testDir2.edgeTypes();
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


//
//    @Test
//    public void testMergeEdgeWithRF() {
//        Node majorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(majorNode, 17);
//        Node minorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(minorNode, 19);
//        majorNode.getEdgeMap(EDGETYPE.RF).clear();
//        majorNode.getEdgeMap(EDGETYPE.RR).clear();
//
//        minorNode.getEdgeMap(EDGETYPE.RF).clear();
//        minorNode.getEdgeMap(EDGETYPE.RR).clear();
//
//        majorNode.mergeEdges(EDGETYPE.RF, minorNode);
//        NodeFixedTest.compareEdgeMap(majorNode.getEdgeMap(EDGETYPE.RF), minorNode.getEdgeMap(EDGETYPE.FF));
//        NodeFixedTest.compareEdgeMap(majorNode.getEdgeMap(EDGETYPE.RR), minorNode.getEdgeMap(EDGETYPE.FR));
//    }
//
//    @Test
//    public void testMergeEdgeWithRR() {
//        Node majorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(majorNode, 17);
//        Node minorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(minorNode, 19);
//        majorNode.getEdgeMap(EDGETYPE.RR).clear();
//        majorNode.getEdgeMap(EDGETYPE.RF).clear();
//
//        minorNode.getEdgeMap(EDGETYPE.FF).clear();
//        minorNode.getEdgeMap(EDGETYPE.FR).clear();
//
//        majorNode.mergeEdges(EDGETYPE.RR, minorNode);
//        NodeFixedTest.compareEdgeMap(majorNode.getEdgeMap(EDGETYPE.RF), minorNode.getEdgeMap(EDGETYPE.RF));
//        NodeFixedTest.compareEdgeMap(majorNode.getEdgeMap(EDGETYPE.RR), minorNode.getEdgeMap(EDGETYPE.RR));
//    }
//
//    @Test
//    public void testMergeStartAndEndReadIDsWithFF() {
//        Node majorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
//        Node minorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
//        Kmer fixedKmer = new Kmer();
//        fixedKmer.setGlobalKmerLength(13);
//        ReadHeadSet expectedStartReads = new ReadHeadSet(majorNode.getUnflippedReadIds());
//        ReadHeadSet expectedEndReads = new ReadHeadSet(majorNode.getFlippedReadIds());
//        int newOtherOffset = majorNode.getKmerLength() - fixedKmer.getKmerLength() + 1;
//        for (ReadHeadInfo p : minorNode.getUnflippedReadIds()) {
//            expectedStartReads.add(p.getMateId(), p.getReadId(), newOtherOffset + p.getOffset());
//        }
//        for (ReadHeadInfo p : minorNode.getFlippedReadIds()) {
//            expectedEndReads.add(p.getMateId(), p.getReadId(), newOtherOffset + p.getOffset());
//        }
//        majorNode.mergeUnflippedAndFlippedReadIDs(EDGETYPE.FF, minorNode);
//        NodeFixedTest.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
//        NodeFixedTest.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
//    }
//
//    @Test
//    public void testMergeStartAndEndReadIDsWithFR() {
//        Node majorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
//        Node minorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
//        Kmer fixedKmer = new Kmer();
//        fixedKmer.setGlobalKmerLength(13);
//        ReadHeadSet expectedStartReads = new ReadHeadSet(majorNode.getUnflippedReadIds());
//        ReadHeadSet expectedEndReads = new ReadHeadSet(majorNode.getFlippedReadIds());
//        int newOtherOffset = majorNode.getKmerLength() - fixedKmer.getKmerLength() + minorNode.getKmerLength();
//        for (ReadHeadInfo p : minorNode.getFlippedReadIds()) {
//            expectedStartReads.add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset());
//        }
//        for (ReadHeadInfo p : minorNode.getUnflippedReadIds()) {
//            expectedEndReads.add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset());
//        }
//        majorNode.mergeUnflippedAndFlippedReadIDs(EDGETYPE.FR, minorNode);
//        NodeFixedTest.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
//        NodeFixedTest.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
//    }
//
//    @Test
//    public void testMergeStartAndEndReadIDsWithRF() {
//        Node majorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
//        Node minorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
//        Kmer fixedKmer = new Kmer();
//        fixedKmer.setGlobalKmerLength(13);
//        ReadHeadSet expectedStartReads = new ReadHeadSet();
//        ReadHeadSet expectedEndReads = new ReadHeadSet();
//        int newThisOffset = minorNode.getKmerLength() - fixedKmer.getKmerLength() + 1;
//        int newOtherOffset = minorNode.getKmerLength() - 1;
//        for (ReadHeadInfo p : majorNode.getUnflippedReadIds()) {
//            expectedStartReads.add(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
//        }
//        for (ReadHeadInfo p : majorNode.getFlippedReadIds()) {
//            expectedEndReads.add(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
//        }
//        for (ReadHeadInfo p : minorNode.getUnflippedReadIds()) {
//            expectedEndReads.add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset());
//        }
//        for (ReadHeadInfo p : minorNode.getFlippedReadIds()) {
//            expectedStartReads.add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset());
//        }
//        majorNode.mergeUnflippedAndFlippedReadIDs(EDGETYPE.RF, minorNode);
//        NodeFixedTest.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
//        NodeFixedTest.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
//    }
//
//    @Test
//    public void testMergeStartAndEndReadIDsWithRR() {
//        Node majorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
//        Node minorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
//        Kmer fixedKmer = new Kmer();
//        fixedKmer.setGlobalKmerLength(13);
//
//        ReadHeadSet expectedStartReads = new ReadHeadSet();
//        ReadHeadSet expectedEndReads = new ReadHeadSet();
//        int newThisOffset = minorNode.getKmerLength() - fixedKmer.getKmerLength() + 1;
//        for (ReadHeadInfo p : majorNode.getUnflippedReadIds()) {
//            expectedStartReads.add(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
//        }
//        for (ReadHeadInfo p : majorNode.getFlippedReadIds()) {
//            expectedEndReads.add(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
//        }
//        for (ReadHeadInfo p : minorNode.getUnflippedReadIds()) {
//            expectedStartReads.add(p.getMateId(), p.getReadId(), p.getOffset());
//        }
//        for (ReadHeadInfo p : minorNode.getFlippedReadIds()) {
//            expectedEndReads.add(p.getMateId(), p.getReadId(), p.getOffset());
//        }
//        majorNode.mergeUnflippedAndFlippedReadIDs(EDGETYPE.RR, minorNode);
//        NodeFixedTest.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
//        NodeFixedTest.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
//    }
//
//    @Test
//    public void testAddEdgesWithNoFlips() {
//        Node majorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
//        Node minorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
//        EdgeMap expectedFF = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.FF));
//        EdgeMap expectedFR = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.FR));
//        EdgeMap expectedRF = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.RF));
//        EdgeMap expectedRR = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.RR));
//        expectedFF.unionUpdate(minorNode.getEdgeMap(EDGETYPE.FF));
//        expectedFR.unionUpdate(minorNode.getEdgeMap(EDGETYPE.FR));
//        expectedRF.unionUpdate(minorNode.getEdgeMap(EDGETYPE.RF));
//        expectedRR.unionUpdate(minorNode.getEdgeMap(EDGETYPE.RR));
//        majorNode.addEdges(false, minorNode);
//        NodeFixedTest.compareEdgeMap(expectedFF, majorNode.getEdgeMap(EDGETYPE.FF));
//        NodeFixedTest.compareEdgeMap(expectedFR, majorNode.getEdgeMap(EDGETYPE.FR));
//        NodeFixedTest.compareEdgeMap(expectedRF, majorNode.getEdgeMap(EDGETYPE.RF));
//        NodeFixedTest.compareEdgeMap(expectedRR, majorNode.getEdgeMap(EDGETYPE.RR));
//    }
//
//    @Test
//    public void testAddEdgesWithFlips() {
//        Node majorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
//        Node minorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
//
//        EdgeMap expectedFF = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.FF));
//        EdgeMap expectedFR = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.FR));
//        EdgeMap expectedRF = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.RF));
//        EdgeMap expectedRR = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.RR));
//        expectedFF.unionUpdate(minorNode.getEdgeMap(EDGETYPE.RF));
//        expectedFR.unionUpdate(minorNode.getEdgeMap(EDGETYPE.RR));
//        expectedRF.unionUpdate(minorNode.getEdgeMap(EDGETYPE.FF));
//        expectedRR.unionUpdate(minorNode.getEdgeMap(EDGETYPE.FR));
//        majorNode.addEdges(true, minorNode);
//        NodeFixedTest.compareEdgeMap(expectedFF, majorNode.getEdgeMap(EDGETYPE.FF));
//        NodeFixedTest.compareEdgeMap(expectedFR, majorNode.getEdgeMap(EDGETYPE.FR));
//        NodeFixedTest.compareEdgeMap(expectedRF, majorNode.getEdgeMap(EDGETYPE.RF));
//        NodeFixedTest.compareEdgeMap(expectedRR, majorNode.getEdgeMap(EDGETYPE.RR));
//    }
//
//    @Test
//    public void testAddStartAndEndWithNoFlip() {
//        Node majorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
//        Node minorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
//        Kmer fixedKmer = new Kmer();
//        fixedKmer.setGlobalKmerLength(13);
//
//        ReadHeadSet expectedStartReads = new ReadHeadSet(majorNode.getUnflippedReadIds());
//        ReadHeadSet expectedEndReads = new ReadHeadSet(majorNode.getFlippedReadIds());
//        float lengthFactor = (float) majorNode.getInternalKmer().getKmerLetterLength()
//                / (float) minorNode.getInternalKmer().getKmerLetterLength();
//        for (ReadHeadInfo p : minorNode.getUnflippedReadIds()) {
//            expectedStartReads.add(p.getMateId(), p.getReadId(), (int) (p.getOffset() * lengthFactor));
//        }
//        for (ReadHeadInfo p : minorNode.getFlippedReadIds()) {
//            expectedEndReads.add(p.getMateId(), p.getReadId(), (int) (p.getOffset() * lengthFactor));
//        }
//        majorNode.addUnflippedAndFlippedReadIds(false, minorNode);
//        NodeFixedTest.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
//        NodeFixedTest.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
//    }
//
//    @Test
//    public void testAddStartAndEndWithFlip() {
//        Node majorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
//        Node minorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
//        Kmer fixedKmer = new Kmer();
//        fixedKmer.setGlobalKmerLength(13);
//
//        ReadHeadSet expectedStartReads = new ReadHeadSet(majorNode.getUnflippedReadIds());
//        ReadHeadSet expectedEndReads = new ReadHeadSet(majorNode.getFlippedReadIds());
//        float lengthFactor = (float) majorNode.getInternalKmer().getKmerLetterLength()
//                / (float) minorNode.getInternalKmer().getKmerLetterLength();
//        int newPOffset;
//        for (ReadHeadInfo p : minorNode.getUnflippedReadIds()) {
//            newPOffset = minorNode.getInternalKmer().getKmerLetterLength() - 1 - p.getOffset();
//            expectedEndReads.add(p.getMateId(), p.getReadId(), (int) (newPOffset * lengthFactor));
//        }
//        for (ReadHeadInfo p : minorNode.getFlippedReadIds()) {
//            newPOffset = minorNode.getInternalKmer().getKmerLetterLength() - 1 - p.getOffset();
//            expectedStartReads.add(p.getMateId(), p.getReadId(), (int) (newPOffset * lengthFactor));
//        }
//        majorNode.addUnflippedAndFlippedReadIds(true, minorNode);
//        NodeFixedTest.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
//        NodeFixedTest.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
//    }
//
//    @Test
//    public void testUpdateEdges() {
//        Node majorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(majorNode, 18);
//        Node minorNode = new Node();
//        NodeFixedTest.assembleNodeRandomly(minorNode, 20);
//        Kmer fixedKmer = new Kmer();
//        fixedKmer.setGlobalKmerLength(13);
//        int ffEdgeCount = majorNode.getEdgeMap(EDGETYPE.FF).size() / 2;
//        ArrayList<Map.Entry<VKmer, ReadIdSet>> iterFFList = new ArrayList<Map.Entry<VKmer, ReadIdSet>>();
//        iterFFList.addAll(majorNode.getEdgeMap(EDGETYPE.FF).entrySet());
//
//        int frEdgeCount = majorNode.getEdgeMap(EDGETYPE.FR).size() / 2;
//        ArrayList<Map.Entry<VKmer, ReadIdSet>> iterFRList = new ArrayList<Map.Entry<VKmer, ReadIdSet>>();
//        iterFRList.addAll(majorNode.getEdgeMap(EDGETYPE.FR).entrySet());
//
//        int rfEdgeCount = majorNode.getEdgeMap(EDGETYPE.RF).size() / 2;
//        ArrayList<Map.Entry<VKmer, ReadIdSet>> iterRFList = new ArrayList<Map.Entry<VKmer, ReadIdSet>>();
//        iterRFList.addAll(majorNode.getEdgeMap(EDGETYPE.RF).entrySet());
//
//        int rrEdgeCount = majorNode.getEdgeMap(EDGETYPE.RR).size() / 2;
//        ArrayList<Map.Entry<VKmer, ReadIdSet>> iterRRList = new ArrayList<Map.Entry<VKmer, ReadIdSet>>();
//        iterRRList.addAll(majorNode.getEdgeMap(EDGETYPE.RR).entrySet());
//
//        EdgeMap expectedFF = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.FF));
//        EdgeMap expectedFR = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.FR));
//        EdgeMap expectedRF = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.RF));
//        EdgeMap expectedRR = new EdgeMap(majorNode.getEdgeMap(EDGETYPE.RR));
//
//        expectedFF.remove(iterFFList.get(ffEdgeCount).getKey());
//        expectedFF.unionUpdate(minorNode.getEdgeMap(EDGETYPE.FF));
//
//        expectedFR.remove(iterFRList.get(frEdgeCount).getKey());
//        expectedFR.unionUpdate(minorNode.getEdgeMap(EDGETYPE.FR));
//
//        expectedRF.remove(iterRFList.get(rfEdgeCount).getKey());
//        expectedRF.unionUpdate(minorNode.getEdgeMap(EDGETYPE.RF));
//
//        expectedRR.remove(iterRRList.get(rrEdgeCount).getKey());
//        expectedRR.unionUpdate(minorNode.getEdgeMap(EDGETYPE.RR));
//
//        majorNode.updateEdges(EDGETYPE.FF, iterFFList.get(ffEdgeCount).getKey(), EDGETYPE.FF, EDGETYPE.FF, minorNode,
//                true);
//        majorNode.updateEdges(EDGETYPE.FR, iterFRList.get(frEdgeCount).getKey(), EDGETYPE.FR, EDGETYPE.FR, minorNode,
//                true);
//        majorNode.updateEdges(EDGETYPE.RF, iterRFList.get(rfEdgeCount).getKey(), EDGETYPE.RF, EDGETYPE.RF, minorNode,
//                true);
//        majorNode.updateEdges(EDGETYPE.RR, iterRRList.get(rrEdgeCount).getKey(), EDGETYPE.RR, EDGETYPE.RR, minorNode,
//                true);
//        NodeFixedTest.compareEdgeMap(expectedFF, majorNode.getEdgeMap(EDGETYPE.FF));
//        NodeFixedTest.compareEdgeMap(expectedFR, majorNode.getEdgeMap(EDGETYPE.FR));
//        NodeFixedTest.compareEdgeMap(expectedRF, majorNode.getEdgeMap(EDGETYPE.RF));
//        NodeFixedTest.compareEdgeMap(expectedRR, majorNode.getEdgeMap(EDGETYPE.RR));
//    }
//
//    @Test
//    public void testDegree() {
//        Node node1 = new Node();
//        NodeFixedTest.assembleNodeRandomly(node1, 20);
//        Node node2 = new Node();
//        NodeFixedTest.assembleNodeRandomly(node2, 21);
//        Node node3 = new Node();
//        NodeFixedTest.assembleNodeRandomly(node3, 22);
//        Node node4 = new Node();
//        NodeFixedTest.assembleNodeRandomly(node4, 23);
//
//        Assert.assertEquals(node1.getEdgeMap(EDGETYPE.FF).size() + node1.getEdgeMap(EDGETYPE.FR).size(),
//                node1.degree(DIR.FORWARD));
//        Assert.assertEquals(node1.getEdgeMap(EDGETYPE.FF).size() + node1.getEdgeMap(EDGETYPE.FR).size(),
//                node1.degree(DIR.FORWARD));
//        Assert.assertEquals(node1.getEdgeMap(EDGETYPE.RF).size() + node1.getEdgeMap(EDGETYPE.RR).size(),
//                node1.degree(DIR.REVERSE));
//        Assert.assertEquals(node1.getEdgeMap(EDGETYPE.RF).size() + node1.getEdgeMap(EDGETYPE.RR).size(),
//                node1.degree(DIR.REVERSE));
//    }
//
//    @Test
//    public void testInAndOutdegree() {
//        Node node = new Node();
//        NodeFixedTest.assembleNodeRandomly(node, 20);
//        Assert.assertEquals(node.getEdgeMap(EDGETYPE.FF).size() + node.getEdgeMap(EDGETYPE.FR).size(), node.outDegree());
//        Assert.assertEquals(node.getEdgeMap(EDGETYPE.RF).size() + node.getEdgeMap(EDGETYPE.RR).size(), node.inDegree());
//    }
//
//    @Test
//    public void testIsPathNode() {
//        Node node = new Node();
//        NodeFixedTest.assembleNodeRandomly(node, 20);
//        Assert.assertEquals(false, node.isPathNode());
//        node.getEdgeMap(EDGETYPE.FR).clear();
//        node.getEdgeMap(EDGETYPE.RF).clear();
//        int totalSize2 = node.getEdgeMap(EDGETYPE.FF).size();
//        for (int i = 0; i < totalSize2 - 1; i++)
//            node.getEdgeMap(EDGETYPE.FF).pollFirstEntry();
//
//        int totalSize = node.getEdgeMap(EDGETYPE.RR).size();
//        for (int i = 0; i < totalSize - 1; i++)
//            node.getEdgeMap(EDGETYPE.RR).pollFirstEntry();
//        Assert.assertEquals(true, node.isPathNode());
//    }
//
//    @Test
//    public void testIsSimpleOrTerminalPath() {
//        Node node = new Node();
//        NodeFixedTest.assembleNodeRandomly(node, 20);
//        Assert.assertEquals(false, node.isPathNode());
//        node.getEdgeMap(EDGETYPE.FR).clear();
//        node.getEdgeMap(EDGETYPE.RF).clear();
//        node.getEdgeMap(EDGETYPE.RR).clear();
//        int totalSize2 = node.getEdgeMap(EDGETYPE.FF).size();
//        for (int i = 0; i < totalSize2 - 1; i++)
//            node.getEdgeMap(EDGETYPE.FF).pollFirstEntry();
//        Assert.assertEquals(true, node.isSimpleOrTerminalPath());
//
//        Node node2 = new Node();
//        NodeFixedTest.assembleNodeRandomly(node, 20);
//        Assert.assertEquals(false, node.isPathNode());
//        node.getEdgeMap(EDGETYPE.FR).clear();
//        node.getEdgeMap(EDGETYPE.FF).clear();
//        node.getEdgeMap(EDGETYPE.RR).clear();
//        int totalSize1 = node.getEdgeMap(EDGETYPE.RF).size();
//        for (int i = 0; i < totalSize1 - 1; i++)
//            node.getEdgeMap(EDGETYPE.RF).pollFirstEntry();
//        Assert.assertEquals(true, node.isSimpleOrTerminalPath());
//    }
}
