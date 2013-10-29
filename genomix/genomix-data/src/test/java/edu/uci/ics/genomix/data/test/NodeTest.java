package edu.uci.ics.genomix.data.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import junit.framework.Assert;
import org.junit.Test;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.Node.NeighborInfo;
import edu.uci.ics.genomix.type.Node.READHEAD_ORIENTATION;
import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.ReadHeadSet;
import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.util.Marshal;

public class NodeTest {

//    private static final char[] symbols = new char[4];
//    static {
//        symbols[0] = 'A';
//        symbols[1] = 'C';
//        symbols[2] = 'G';
//        symbols[3] = 'T';
//    }
//
//    public static String generateString(int length) {
//        Random random = new Random();
//        char[] buf = new char[length];
//        for (int idx = 0; idx < buf.length; idx++) {
//            buf[idx] = symbols[random.nextInt(4)];
//        }
//        return new String(buf);
//    }
//
//    public static void assembleNodeRandomly(Node targetNode, int orderNum) {
//        String srcInternalStr = generateString(orderNum);
//        //        System.out.println(srcInternalStr.length());
//        VKmer srcInternalKmer = new VKmer(srcInternalStr);
//        //        System.out.println(srcInternalKmer.getKmerLetterLength());
//        int min = 2;
//        int max = 3;
//        ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList;
//        SimpleEntry<VKmer, ReadIdSet> edgeId;
//        EdgeMap edge;
//        for (EDGETYPE e : EDGETYPE.values()) {
//            sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
//            for (int i = 0; i < min + (int) (Math.random() * ((max - min) + 1)); i++) {
//                String edgeStr = generateString(orderNum);
//                VKmer edgeKmer = new VKmer(edgeStr);
//                ReadIdSet edgeIdSet = new ReadIdSet();
//                for (long j = 0; j < min + (int) (Math.random() * ((max - min) + 1)); j++) {
//                    edgeIdSet.add(j);
//                }
//                edgeId = new SimpleEntry<VKmer, ReadIdSet>(edgeKmer, edgeIdSet);
//                sampleList.add(edgeId);
//            }
//            edge = new EdgeMap(sampleList);
//            targetNode.setEdgeMap(e, edge);
//        }
//        ReadHeadSet startReads = new ReadHeadSet();
//        ReadHeadSet endReads = new ReadHeadSet();
//        for (int i = 0; i < min + (int) (Math.random() * ((max - min) + 1)); i++) {
//            startReads.add((byte) 1, (long) orderNum + i, i);
//            endReads.add((byte) 0, (long) orderNum + i, i);
//        }
//        targetNode.setStartReads(startReads);
//        targetNode.setEndReads(endReads);
//        targetNode.setInternalKmer(srcInternalKmer);
//        targetNode.setAverageCoverage((float) (orderNum * (min + (int) (Math.random() * ((max - min) + 1)))));
//    }
//
//    public static void printSrcNodeInfo(Node srcNode) {
//        System.out.println("InternalKmer: " + srcNode.getInternalKmer().toString());
//        for (EDGETYPE e : EDGETYPE.values()) {
//            System.out.println(e.toString());
//            for (Map.Entry<VKmer, ReadIdSet> iter : srcNode.getEdgeMap(e).entrySet()) {
//                System.out.println("edgeKmer: " + iter.getKey().toString());
//                for (Long readidIter : iter.getValue())
//                    System.out.print(readidIter.toString() + "  ");
//                System.out.println("");
//            }
//            System.out.println("-------------------------------------");
//        }
//        System.out.println("StartReads");
//        for (ReadHeadInfo startIter : srcNode.getStartReads())
//            System.out.println(startIter.toString() + "---");
//        System.out.println("");
//        System.out.println("EndsReads");
//        for (ReadHeadInfo startIter : srcNode.getEndReads())
//            System.out.println(startIter.toString() + "---");
//        System.out.println("");
//        System.out.println("Coverage: " + srcNode.getAverageCoverage());
//        System.out.println("***************************************");
//    }
//
//    public static void compareTwoNodes(Node et1, Node et2) {
//        Assert.assertEquals(et1.getInternalKmer().toString(), et2.getInternalKmer().toString());
//        for (EDGETYPE e : EDGETYPE.values()) {
//            Assert.assertEquals(et1.getEdgeMap(e).size(), et2.getEdgeMap(e).size());
//            for (Map.Entry<VKmer, ReadIdSet> iter1 : et1.getEdgeMap(e).entrySet()) {
//                Map.Entry<VKmer, ReadIdSet> iter2 = et2.getEdgeMap(e).pollFirstEntry();
//                Assert.assertEquals(iter1.getKey().toString(), iter2.getKey().toString());
//                for (Long readidIter1 : iter1.getValue()) {
//                    Long readidIter2 = iter2.getValue().pollFirst();
//                    Assert.assertEquals(readidIter1.toString(), readidIter2.toString());
//                }
//            }
//        }
//        for (ReadHeadInfo startIter1 : et1.getStartReads()) {
//            ReadHeadInfo startIter2 = et2.getStartReads().pollFirst();
//            Assert.assertEquals(startIter1.toString(), startIter2.toString());
//        }
//        for (ReadHeadInfo endIter1 : et1.getEndReads()) {
//            ReadHeadInfo endIter2 = et2.getEndReads().pollFirst();
//            Assert.assertEquals(endIter1.toString(), endIter2.toString());
//        }
//    }
//
//    public static void getEdgeMapRandomly(EdgeMap edgeMap, int orderNum) {
//        int min = 3;
//        int max = 4;
//        ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList;
//        SimpleEntry<VKmer, ReadIdSet> edgeId;
//        for (EDGETYPE e : EDGETYPE.values()) {
//            sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
//            for (int i = 0; i < min + (int) (Math.random() * ((max - min) + 1)); i++) {
//                String edgeStr = generateString(orderNum);
//                VKmer edgeKmer = new VKmer(edgeStr);
//                ReadIdSet edgeIdSet = new ReadIdSet();
//                for (long j = 0; j < min + (int) (Math.random() * ((max - min) + 1)); j++) {
//                    edgeIdSet.add(j);
//                }
//                edgeId = new SimpleEntry<VKmer, ReadIdSet>(edgeKmer, edgeIdSet);
//                sampleList.add(edgeId);
//            }
//            edgeMap = new EdgeMap(sampleList);
//        }
//
//    }
//
//    public static void compareEdgeMap(EdgeMap et1, EdgeMap et2) {
//        Assert.assertEquals(et1.size(), et2.size());
//        for (Map.Entry<VKmer, ReadIdSet> iter1 : et1.entrySet()) {
//            Map.Entry<VKmer, ReadIdSet> iter2 = et2.pollFirstEntry();
//            Assert.assertEquals(iter1.getKey().toString(), iter2.getKey().toString());
//            for (Long readidIter1 : iter1.getValue()) {
//                Long readidIter2 = iter2.getValue().pollFirst();
//                Assert.assertEquals(readidIter1.toString(), readidIter2.toString());
//            }
//        }
//    }
//
//    public static void getStartReadsAndEndReadsRandomly(ReadHeadSet readSet, int orderNum) {
//        int min = 3;
//        int max = 5;
//        for (int i = 0; i < min + (int) (Math.random() * ((max - min) + 1)); i++) {
//            readSet.add((byte) 1, (long) orderNum + i, i);
//        }
//    }
//
//    public static void compareStartReadsAndEndReads(ReadHeadSet et1, ReadHeadSet et2) {
//        Assert.assertEquals(et1.size(), et2.size());
//        for (ReadHeadInfo iter1 : et1) {
//            ReadHeadInfo iter2 = et2.pollFirst();
//            Assert.assertEquals(iter1.toString(), iter2.toString());
//        }
//    }
//
//    /**
//     * basic checking for enum DIR in Node class
//     * 
//     * @throws IOException
//     */
//    @Test
//    public void testDIR() throws IOException {
//        Assert.assertEquals(0b01 << 2, DIR.REVERSE.get());
//        Assert.assertEquals(0b10 << 2, DIR.FORWARD.get());
//        DIR testDir1 = DIR.FORWARD;
//        DIR testDir2 = DIR.REVERSE;
//        Assert.assertEquals(DIR.REVERSE, testDir1.mirror());
//        Assert.assertEquals(DIR.FORWARD, testDir2.mirror());
//        Assert.assertEquals(0b11 << 2, DIR.fromSet(EnumSet.allOf(DIR.class)));
//        Assert.assertEquals(0b00 << 2, DIR.fromSet(EnumSet.noneOf(DIR.class)));
//
//        EnumSet<EDGETYPE> edgeTypes1 = testDir1.edgeTypes();
//        EnumSet<EDGETYPE> edgeExample1 = EnumSet.noneOf(EDGETYPE.class);
//        EnumSet<EDGETYPE> edgeTypes2 = testDir2.edgeTypes();
//        EnumSet<EDGETYPE> edgeExample2 = EnumSet.noneOf(EDGETYPE.class);
//        edgeExample1.add(EDGETYPE.FF);
//        edgeExample1.add(EDGETYPE.FR);
//        Assert.assertEquals(edgeExample1, edgeTypes1);
//
//        edgeExample2.add(EDGETYPE.RF);
//        edgeExample2.add(EDGETYPE.RR);
//        Assert.assertEquals(edgeExample2, edgeTypes2);
//
//        Assert.assertEquals(edgeExample1, DIR.edgeTypesInDir(testDir1));
//        Assert.assertEquals(edgeExample2, DIR.edgeTypesInDir(testDir2));
//
//        EnumSet<DIR> dirExample = EnumSet.noneOf(DIR.class);
//        dirExample.add(DIR.FORWARD);
//        Assert.assertEquals(dirExample, DIR.enumSetFromByte((short) 8));
//        dirExample.clear();
//        dirExample.add(DIR.REVERSE);
//        Assert.assertEquals(dirExample, DIR.enumSetFromByte((short) 4));
//
//        dirExample.clear();
//        dirExample.add(DIR.FORWARD);
//        Assert.assertEquals(dirExample, DIR.flipSetFromByte((short) 4));
//        dirExample.clear();
//        dirExample.add(DIR.REVERSE);
//        Assert.assertEquals(dirExample, DIR.flipSetFromByte((short) 8));
//    }
//
//    /**
//     * basic checking for EDGETYPE in Node class
//     * 
//     * @throws IOException
//     */
//    @Test
//    public void testEDGETYPE() throws IOException {
//        //fromByte()
//        Assert.assertEquals(EDGETYPE.FF, EDGETYPE.fromByte((byte) 0));
//        Assert.assertEquals(EDGETYPE.FR, EDGETYPE.fromByte((byte) 1));
//        Assert.assertEquals(EDGETYPE.RF, EDGETYPE.fromByte((byte) 2));
//        Assert.assertEquals(EDGETYPE.RR, EDGETYPE.fromByte((byte) 3));
//        //mirror()
//        Assert.assertEquals(EDGETYPE.RR, EDGETYPE.FF.mirror());
//        Assert.assertEquals(EDGETYPE.FR, EDGETYPE.FR.mirror());
//        Assert.assertEquals(EDGETYPE.RF, EDGETYPE.RF.mirror());
//        Assert.assertEquals(EDGETYPE.FF, EDGETYPE.RR.mirror());
//        //DIR()
//        Assert.assertEquals(DIR.FORWARD, EDGETYPE.FF.dir());
//        Assert.assertEquals(DIR.FORWARD, EDGETYPE.FR.dir());
//        Assert.assertEquals(DIR.REVERSE, EDGETYPE.RF.dir());
//        Assert.assertEquals(DIR.REVERSE, EDGETYPE.RR.dir());
//        //resolveEdgeThroughPath()
//        Assert.assertEquals(EDGETYPE.RF,
//                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 0), EDGETYPE.fromByte((byte) 2)));
//        Assert.assertEquals(EDGETYPE.RR,
//                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 0), EDGETYPE.fromByte((byte) 3)));
//
//        Assert.assertEquals(EDGETYPE.FF,
//                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 1), EDGETYPE.fromByte((byte) 2)));
//        Assert.assertEquals(EDGETYPE.FR,
//                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 1), EDGETYPE.fromByte((byte) 3)));
//
//        Assert.assertEquals(EDGETYPE.RF,
//                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 2), EDGETYPE.fromByte((byte) 0)));
//        Assert.assertEquals(EDGETYPE.RR,
//                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 2), EDGETYPE.fromByte((byte) 1)));
//
//        Assert.assertEquals(EDGETYPE.FF,
//                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 3), EDGETYPE.fromByte((byte) 0)));
//        Assert.assertEquals(EDGETYPE.FR,
//                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 3), EDGETYPE.fromByte((byte) 1)));
//        //causeFlip()
//        Assert.assertEquals(false, EDGETYPE.FF.causesFlip());
//        Assert.assertEquals(true, EDGETYPE.FR.causesFlip());
//        Assert.assertEquals(true, EDGETYPE.RF.causesFlip());
//        Assert.assertEquals(false, EDGETYPE.RR.causesFlip());
//        //flipNeighbor()
//        Assert.assertEquals(true, EDGETYPE.sameOrientation(EDGETYPE.FF, EDGETYPE.FR));
//        Assert.assertEquals(false, EDGETYPE.sameOrientation(EDGETYPE.RF, EDGETYPE.FR));
//    }
//
//    @Test
//    public void testREADHEAD_ORIENTATION() throws IOException {
//        Assert.assertEquals(READHEAD_ORIENTATION.FLIPPED, READHEAD_ORIENTATION.fromByte((byte) 1));
//        Assert.assertEquals(READHEAD_ORIENTATION.UNFLIPPED, READHEAD_ORIENTATION.fromByte((byte) 0));
//    }
//
//    @Test
//    public void testNeighborsInfo() throws IOException {
//        String sample1Str = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATC";
//        VKmer oldKSample = new VKmer(sample1Str);
//        SimpleEntry<VKmer, ReadIdSet> sample;
//        ReadIdSet positionsSample = new ReadIdSet();
//        long numelements = 10;
//        for (long i = 0; i < numelements; i++) {
//            positionsSample.add(i);
//        }
//        sample = new SimpleEntry<VKmer, ReadIdSet>(oldKSample, positionsSample);
//        ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
//        sampleList.add(sample);
//
//        String sample2Str = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGAT";
//        VKmer oldKSample2 = new VKmer(sample2Str);
//        SimpleEntry<VKmer, ReadIdSet> sample2;
//        ReadIdSet positionsSample2 = new ReadIdSet();
//        long numelements2 = 20;
//        for (long i = 10; i < numelements2; i++) {
//            positionsSample2.add(i);
//        }
//        sample2 = new SimpleEntry<VKmer, ReadIdSet>(oldKSample2, positionsSample2);
//        sampleList.add(sample2);
//        EdgeMap source = new EdgeMap(sampleList);
//        Node.NeighborsInfo neighborsInfor = new Node.NeighborsInfo(EDGETYPE.FF, source);
//        Iterator<NeighborInfo> iterator = neighborsInfor.iterator();
//        long i = 0;
//        Assert.assertEquals(true, iterator.hasNext());
//        NeighborInfo temp = iterator.next();
//        Assert.assertEquals(EDGETYPE.FF, temp.et);
//        Assert.assertEquals(sample1Str, temp.kmer.toString());
//        for (; i < numelements; i++) {
//            Assert.assertEquals((Long) i, temp.readIds.pollFirst());
//        }
//    }
//
//    @Test
//    public void testNodeReset() throws IOException {
//        String internalStr = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATC";
//        VKmer internalSample = new VKmer(internalStr);
//        String sampleStr = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATC";
//        VKmer oldKSample = new VKmer(sampleStr);
//        SimpleEntry<VKmer, ReadIdSet> sample;
//        ReadIdSet positionsSample = new ReadIdSet();
//        long numelements = 10;
//        for (long i = 0; i < numelements; i++) {
//            positionsSample.add(i);
//        }
//        sample = new SimpleEntry<VKmer, ReadIdSet>(oldKSample, positionsSample);
//        ArrayList<SimpleEntry<VKmer, ReadIdSet>> sampleList = new ArrayList<SimpleEntry<VKmer, ReadIdSet>>();
//        sampleList.add(sample);
//        EdgeMap edge = new EdgeMap(sampleList);
//        //-------------------------------------------
//        ReadHeadSet startReads = new ReadHeadSet();
//        ReadHeadSet endReads = new ReadHeadSet();
//        byte mateId;
//        long readId;
//        int posId;
//        for (int i = 0; i < 5; i++) {
//            mateId = (byte) 1;
//            readId = (long) i;
//            posId = i;
//            startReads.add(mateId, readId, posId);
//            Assert.assertEquals(i + 1, startReads.size());
//        }
//        for (int i = 5; i < 10; i++) {
//            mateId = (byte) 0;
//            readId = (long) i;
//            posId = i;
//            endReads.add(mateId, readId, posId);
//            Assert.assertEquals(i - 5 + 1, endReads.size());
//        }
//        Node node = new Node();
//        node.setInternalKmer(internalSample);
//        node.setEdgeMap(EDGETYPE.RF, edge);
//        node.setAverageCoverage((float) 54.6);
//        node.setStartReads(startReads);
//        node.setEndReads(endReads);
//        node.reset();
//        Assert.assertEquals((float) 0, node.getAverageCoverage());
//        Assert.assertEquals(true, node.getEdgeMap(EDGETYPE.RF).isEmpty());
//        Assert.assertEquals(4, node.getInternalKmer().getLength()); //only left the bytes which contain the header
//        Assert.assertEquals(true, node.getStartReads().isEmpty());
//        Assert.assertEquals(true, node.getEndReads().isEmpty());
//    }
//
//    @Test
//    public void testSetCopyWithNode() throws IOException {
//        Node srcNode = new Node();
//        NodeTest.assembleNodeRandomly(srcNode, 10);
//        Node targetNode = new Node();
//        targetNode.setAsCopy(srcNode);
//        NodeTest.compareTwoNodes(srcNode, targetNode);
//    }
//
//    @Test
//    public void testSetCopyAndRefWithByteArray() throws IOException {
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        Node[] dataNodes = new Node[5];
//        for (int i = 0; i < 5; i++)
//            dataNodes[i] = new Node();
//        int[] nodeOffset = new int[5];
//
//        for (int i = 10; i < 15; i++) {
//            NodeTest.assembleNodeRandomly(dataNodes[i - 10], i);
//            nodeOffset[i - 10] = dataNodes[i - 10].getSerializedLength();
//            outputStream.write(dataNodes[i - 10].marshalToByteArray());
//        }
//        byte[] dataArray = outputStream.toByteArray();
//        Node testCopyNode = new Node();
//        for (int i = 0; i < 5; i++) {
//            int totalOffset = 0;
//            for (int j = 0; j < i; j++) {
//                totalOffset += nodeOffset[j];
//            }
//            testCopyNode.setAsCopy(dataArray, totalOffset);
//            NodeTest.compareTwoNodes(dataNodes[i], testCopyNode);
//        }
//        Node testRefNode = new Node();
//        for (int i = 0; i < 5; i++) {
//            int totalOffset = 0;
//            for (int j = 0; j < i; j++) {
//                totalOffset += nodeOffset[j];
//            }
//            testRefNode.setAsReference(dataArray, totalOffset);
//            NodeTest.compareTwoNodes(dataNodes[i], testRefNode);
//        }
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void testGetNeighborEdgeTypeWithException() {
//        Node testNode = new Node();
//        NodeTest.assembleNodeRandomly(testNode, 20);
//        testNode.getNeighborEdgeType(DIR.FORWARD);
//    }
//
//    @Test
//    public void testGetNeighborEdgeType() {
//        Node testNode = new Node();
//        NodeTest.assembleNodeRandomly(testNode, 20);
//        testNode.getEdgeMap(EDGETYPE.FF).clear();
//        testNode.getEdgeMap(EDGETYPE.FR).clear();
//        testNode.getEdgeMap(EDGETYPE.RF).clear();
//        int totalCount = testNode.getEdgeMap(EDGETYPE.RR).size();
//        for (int i = 0; i < totalCount - 1; i++) {
//            testNode.getEdgeMap(EDGETYPE.RR).pollFirstEntry();
//        }
//        Assert.assertEquals(EDGETYPE.RR, testNode.getNeighborEdgeType(DIR.REVERSE));
//    }
//
//    @Test
//    public void testGetSingleNeighbor() {
//        Node testNode = new Node();
//        NodeTest.assembleNodeRandomly(testNode, 20);
//        Assert.assertEquals(null, testNode.getSingleNeighbor(DIR.FORWARD));
//    }
//
//    @Test
//    public void testSetEdgeMap() {
//        Node testNode = new Node();
//        NodeTest.assembleNodeRandomly(testNode, 20);
//        EdgeMap[] edge = new EdgeMap[4];
//        for (int i = 0; i < 4; i++) {
//            edge[i] = new EdgeMap();
//        }
//        for (int i = 0; i < 4; i++) {
//            getEdgeMapRandomly(edge[i], 10 + i);
//        }
//
//        testNode.setEdgeMap(EDGETYPE.FF, edge[0]);
//        testNode.setEdgeMap(EDGETYPE.FR, edge[1]);
//        testNode.setEdgeMap(EDGETYPE.RF, edge[2]);
//        testNode.setEdgeMap(EDGETYPE.RR, edge[3]);
//        NodeTest.compareEdgeMap(testNode.getEdgeMap(EDGETYPE.FF), edge[0]);
//        NodeTest.compareEdgeMap(testNode.getEdgeMap(EDGETYPE.FR), edge[1]);
//        NodeTest.compareEdgeMap(testNode.getEdgeMap(EDGETYPE.RF), edge[2]);
//        NodeTest.compareEdgeMap(testNode.getEdgeMap(EDGETYPE.RR), edge[3]);
//    }
}