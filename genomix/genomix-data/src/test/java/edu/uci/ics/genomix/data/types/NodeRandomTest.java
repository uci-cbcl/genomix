package edu.uci.ics.genomix.data.types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.AbstractMap.SimpleEntry;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import edu.uci.ics.genomix.data.types.Node.NeighborInfo;

public class NodeRandomTest {

    public static int strMaxLength;
    public static int strMinLength;
    public static int vkmerListNumMax;
    public static int vkmerListNumMin;

    @Before
    public void setUp() {
        strMaxLength = 9;
        strMinLength = 6;
        vkmerListNumMin = 5;
        vkmerListNumMax = 8;
        if ((strMinLength <= 0) || (strMaxLength <= 0)) {
            throw new IllegalArgumentException("strMinLength or strMaxLength can not be less than 0!");
        }
        if (strMinLength > strMaxLength) {
            throw new IllegalArgumentException("strMinLength can not be larger than strMaxLength!");
        }
        if ((vkmerListNumMin <= 0) || (vkmerListNumMax <= 0)) {
            throw new IllegalArgumentException("vkmerListNumMin or vkmerListNumMax can not be less than 0!");
        }
        if (vkmerListNumMin > vkmerListNumMax) {
            throw new IllegalArgumentException("vkmerListNumMin can not be larger than vkmerListNumMax!");
        }
    }

    @Test
    public void testNodeReset() throws IOException {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node srcNode = new Node();
        RandomTestHelper.assembleNodeRandomly(srcNode, strLength, vkmerListNumMin, vkmerListNumMax);
        srcNode.reset();
        Assert.assertEquals(0, srcNode.getEdges(EDGETYPE.RF).size());
        Assert.assertEquals(0, srcNode.getInternalKmer().lettersInKmer);
        Assert.assertEquals(0, srcNode.getUnflippedReadIds().size());
        Assert.assertEquals(0, srcNode.getFlippedReadIds().size());
    }

    @Test
    public void testSetCopyWithNode() throws IOException {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node srcNode = new Node();
        RandomTestHelper.assembleNodeRandomly(srcNode, strLength, vkmerListNumMin, vkmerListNumMax);
        Node targetNode = new Node();
        targetNode.setAsCopy(srcNode);
        RandomTestHelper.compareTwoNodes(srcNode, targetNode);
    }

    @Test
    public void testSetCopyAndRefWithByteArray() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Node[] dataNodes = new Node[5];
        for (int i = 0; i < 5; i++)
            dataNodes[i] = new Node();
        int[] nodeOffset = new int[5];

        for (int i = 10; i < 15; i++) {
            RandomTestHelper.assembleNodeRandomly(dataNodes[i - 10], i, vkmerListNumMin, vkmerListNumMax);
            nodeOffset[i - 10] = dataNodes[i - 10].marshalToByteArray().length;
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
            RandomTestHelper.compareTwoNodes(dataNodes[i], testCopyNode);
        }
        Node testRefNode = new Node();
        for (int i = 0; i < 5; i++) {
            int totalOffset = 0;
            for (int j = 0; j < i; j++) {
                totalOffset += nodeOffset[j];
            }
            testRefNode.setAsReference(dataArray, totalOffset);
            RandomTestHelper.compareTwoNodes(dataNodes[i], testRefNode);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNeighborEdgeTypeWithException() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node testNode = new Node();
        RandomTestHelper.assembleNodeRandomly(testNode, strLength, vkmerListNumMin, vkmerListNumMax);
        testNode.getNeighborEdgeType(DIR.FORWARD);
    }

    @Test
    public void testGetNeighborEdgeType() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node testNode = new Node();
        RandomTestHelper.assembleNodeRandomly(testNode, strLength, vkmerListNumMin, vkmerListNumMax);
        testNode.getEdges(EDGETYPE.FF).clear();
        testNode.getEdges(EDGETYPE.FR).clear();
        testNode.getEdges(EDGETYPE.RF).clear();
        int totalCount = testNode.getEdges(EDGETYPE.RR).size();
        VKmer temp = new VKmer();
        for (int i = 0; i < totalCount - 1; i++) {
            temp.setAsCopy(testNode.getEdges(EDGETYPE.RR).getPosition(0));
            testNode.getEdges(EDGETYPE.RR).remove(temp);
        }
        Assert.assertEquals(EDGETYPE.RR, testNode.getNeighborEdgeType(DIR.REVERSE));
    }

    @Test
    public void testGetSingleNeighbor() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node testNode = new Node();
        RandomTestHelper.assembleNodeRandomly(testNode, strLength, vkmerListNumMin, vkmerListNumMax);
        Assert.assertEquals(null, testNode.getSingleNeighbor(DIR.FORWARD));
    }

    @Test
    public void testSetEdgeMap() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node testNode = new Node();
        RandomTestHelper.assembleNodeRandomly(testNode, strLength, vkmerListNumMin, vkmerListNumMax);
        VKmerList[] edge = new VKmerList[4];
        for (int i = 0; i < 4; i++) {
            edge[i] = new VKmerList();
        }
        for (int i = 0; i < 4; i++) {
            RandomTestHelper.getEdgeMapRandomly(edge[i], 10 + i, vkmerListNumMin, vkmerListNumMax);
        }
        testNode.setEdges(EDGETYPE.FF, edge[0]);
        testNode.setEdges(EDGETYPE.FR, edge[1]);
        testNode.setEdges(EDGETYPE.RF, edge[2]);
        testNode.setEdges(EDGETYPE.RR, edge[3]);
        RandomTestHelper.compareEdgeMap(testNode.getEdges(EDGETYPE.FF), edge[0]);
        RandomTestHelper.compareEdgeMap(testNode.getEdges(EDGETYPE.FR), edge[1]);
        RandomTestHelper.compareEdgeMap(testNode.getEdges(EDGETYPE.RF), edge[2]);
        RandomTestHelper.compareEdgeMap(testNode.getEdges(EDGETYPE.RR), edge[3]);
    }

    @Test
    public void testMergeCoverage() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node testNode1 = new Node();
        RandomTestHelper.assembleNodeRandomly(testNode1, strLength, vkmerListNumMin, vkmerListNumMax);
        Kmer fixedKmer = new Kmer();
        fixedKmer.setGlobalKmerLength(13);
        Node testNode2 = new Node();
        RandomTestHelper.assembleNodeRandomly(testNode2, strLength, vkmerListNumMin, vkmerListNumMax);
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
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node testNode1 = new Node();
        RandomTestHelper.assembleNodeRandomly(testNode1, strLength, vkmerListNumMin, vkmerListNumMax);
        Kmer fixedKmer = new Kmer();
        fixedKmer.setGlobalKmerLength(13);
        Node testNode2 = new Node();
        RandomTestHelper.assembleNodeRandomly(testNode2, strLength, vkmerListNumMin, vkmerListNumMax);
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
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        ReadHeadSet[] startAndEndArray = new ReadHeadSet[2];
        for (int i = 0; i < 2; i++)
            startAndEndArray[i] = new ReadHeadSet();
        RandomTestHelper.getUnflippedReadIdsAndEndReadsRandomly(startAndEndArray[0], strLength, vkmerListNumMin,
                vkmerListNumMax);
        RandomTestHelper.getUnflippedReadIdsAndEndReadsRandomly(startAndEndArray[1], strLength, vkmerListNumMin,
                vkmerListNumMax);
        Node testNode = new Node();
        RandomTestHelper.assembleNodeRandomly(testNode, strLength, vkmerListNumMin, vkmerListNumMax);
        testNode.setUnflippedReadIds(startAndEndArray[0]);
        testNode.setFlippedReadIds(startAndEndArray[1]);
        RandomTestHelper.compareStartReadsAndEndReads(startAndEndArray[0], testNode.getUnflippedReadIds());
        RandomTestHelper.compareStartReadsAndEndReads(startAndEndArray[1], testNode.getFlippedReadIds());
    }

    @Test
    public void testWriteAndReadFields() throws IOException {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node srcNode = new Node();
        RandomTestHelper.assembleNodeRandomly(srcNode, strLength, vkmerListNumMin, vkmerListNumMax);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(srcNode.marshalToByteArray().length);
        DataOutputStream out = new DataOutputStream(baos);
        srcNode.write(out);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(inputStream);
        Node testNode = new Node();
        testNode.readFields(in);
        RandomTestHelper.compareTwoNodes(srcNode, testNode);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeEdgeWithFFException() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node majorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(majorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        Node minorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(minorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        majorNode.mergeEdges(EDGETYPE.FF, minorNode);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeEdgeWithFRException() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node majorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(majorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        Node minorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(minorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        majorNode.mergeEdges(EDGETYPE.FF, minorNode);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeEdgeWithRFException() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node majorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(majorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        Node minorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(minorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        majorNode.mergeEdges(EDGETYPE.FF, minorNode);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeEdgeWithRRException() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node majorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(majorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        Node minorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(minorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        majorNode.mergeEdges(EDGETYPE.FF, minorNode);
    }

    @Test
    public void testMergeEdgeWithFF() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node majorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(majorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        Node minorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(minorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        majorNode.getEdges(EDGETYPE.FF).clear();
        majorNode.getEdges(EDGETYPE.FR).clear();
        minorNode.getEdges(EDGETYPE.RF).clear();
        minorNode.getEdges(EDGETYPE.RR).clear();
        majorNode.mergeEdges(EDGETYPE.FF, minorNode);
        RandomTestHelper.compareEdgeMap(majorNode.getEdges(EDGETYPE.FF), minorNode.getEdges(EDGETYPE.FF));
        RandomTestHelper.compareEdgeMap(majorNode.getEdges(EDGETYPE.FR), minorNode.getEdges(EDGETYPE.FR));
    }

    @Test
    public void testMergeEdgeWithFR() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node majorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(majorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        Node minorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(minorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        majorNode.getEdges(EDGETYPE.FF).clear();
        majorNode.getEdges(EDGETYPE.FR).clear();
        minorNode.getEdges(EDGETYPE.FF).clear();
        minorNode.getEdges(EDGETYPE.FR).clear();
        majorNode.mergeEdges(EDGETYPE.FR, minorNode);
        RandomTestHelper.compareEdgeMap(majorNode.getEdges(EDGETYPE.FF), minorNode.getEdges(EDGETYPE.RF));
        RandomTestHelper.compareEdgeMap(majorNode.getEdges(EDGETYPE.FR), minorNode.getEdges(EDGETYPE.RR));
    }

    @Test
    public void testMergeEdgeWithRF() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node majorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(majorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        Node minorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(minorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        majorNode.getEdges(EDGETYPE.RF).clear();
        majorNode.getEdges(EDGETYPE.RR).clear();

        minorNode.getEdges(EDGETYPE.RF).clear();
        minorNode.getEdges(EDGETYPE.RR).clear();

        majorNode.mergeEdges(EDGETYPE.RF, minorNode);
        RandomTestHelper.compareEdgeMap(majorNode.getEdges(EDGETYPE.RF), minorNode.getEdges(EDGETYPE.FF));
        RandomTestHelper.compareEdgeMap(majorNode.getEdges(EDGETYPE.RR), minorNode.getEdges(EDGETYPE.FR));
    }

    @Test
    public void testMergeEdgeWithRR() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node majorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(majorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        Node minorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(minorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        majorNode.getEdges(EDGETYPE.RR).clear();
        majorNode.getEdges(EDGETYPE.RF).clear();

        minorNode.getEdges(EDGETYPE.FF).clear();
        minorNode.getEdges(EDGETYPE.FR).clear();

        majorNode.mergeEdges(EDGETYPE.RR, minorNode);
        RandomTestHelper.compareEdgeMap(majorNode.getEdges(EDGETYPE.RF), minorNode.getEdges(EDGETYPE.RF));
        RandomTestHelper.compareEdgeMap(majorNode.getEdges(EDGETYPE.RR), minorNode.getEdges(EDGETYPE.RR));
    }

    @Test
    public void testMergeStartAndEndReadIDsWithFF() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        Node majorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(majorNode, strLength, vkmerListNumMin, vkmerListNumMax);
        Node minorNode = new Node();
        RandomTestHelper.assembleNodeRandomly(minorNode, strLength, vkmerListNumMin, vkmerListNumMax);
//        Kmer fixedKmer = new Kmer();
//        fixedKmer.setGlobalKmerLength(13);
//        ReadHeadSet expectedStartReads = new ReadHeadSet();
//        expectedStartReads.setAsCopy(majorNode.getUnflippedReadIds());
//        ReadHeadSet expectedEndReads = new ReadHeadSet();
//        expectedEndReads.setAsCopy(majorNode.getFlippedReadIds());
//        int newOtherOffset = majorNode.getKmerLength() - fixedKmer.getKmerLength() + 1;
//        for (ReadHeadInfo p : minorNode.getUnflippedReadIds().getOffSetRange(0, minorNode.getUnflippedReadIds().size())) {
//            System.out.println(newOtherOffset + p.getOffset());
//            expectedStartReads.add(p.getMateId(), p.getLibraryId(), p.getReadId(), newOtherOffset + p.getOffset(),
//                    null, null);
            
//        }
//        System.out.println("--------------------------------------");
//        for (ReadHeadInfo p : minorNode.getFlippedReadIds().getOffSetRange(0, minorNode.getFlippedReadIds().size())) {
//            expectedEndReads.add(p.getMateId(), p.getLibraryId(), p.getReadId(), newOtherOffset + p.getOffset(), null,
//                    null);
//            System.out.println(newOtherOffset + p.getOffset());
//        }
        majorNode.mergeUnflippedAndFlippedReadIDs(EDGETYPE.FF, minorNode);
//        RandomTestHelper.printSrcNodeInfo(majorNode);
//        RandomTestHelper.compareStartReadsAndEndReads(expectedStartReads, majorNode.getUnflippedReadIds());
//        RandomTestHelper.compareStartReadsAndEndReads(expectedEndReads, majorNode.getFlippedReadIds());
    }
}
