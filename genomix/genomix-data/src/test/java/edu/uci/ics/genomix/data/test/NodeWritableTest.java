package edu.uci.ics.genomix.data.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerListWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

public class NodeWritableTest {

    @Test
    public void testNodeReset() throws IOException {
        
        NodeWritable outputNode = new NodeWritable();
        NodeWritable inputNode = new NodeWritable();
        
        KmerListWritable nextKmerList = new KmerListWritable();
        KmerListWritable preKmerList = new KmerListWritable();
        KmerBytesWritable preKmer = new KmerBytesWritable();
        KmerBytesWritable curKmer = new KmerBytesWritable();
        KmerBytesWritable nextKmer = new KmerBytesWritable();
        PositionWritable nodeId = new PositionWritable();
        PositionListWritable nodeIdList = new PositionListWritable();
        KmerBytesWritable.setGlobalKmerLength(5);
        
        nodeId.set((byte)0, (long)1, 0);
        nodeIdList.append(nodeId);
        for (int i = 6; i <= 10; i++) {
        NodeWritable tempNode = new NodeWritable();
        
        String randomString = generaterRandomString(i);
        byte[] array = randomString.getBytes();
        
        curKmer.setByRead(array, 0);
        preKmer.setAsCopy(curKmer);
        nextKmer.setAsCopy(curKmer);
        nextKmer.shiftKmerWithNextChar(array[5]);
        
        nextKmerList.append(nextKmer);
        
        outputNode.setNodeIdList(nodeIdList);
        outputNode.setFFList(nextKmerList);
        
        tempNode.setNodeIdList(nodeIdList);
        tempNode.setFFList(nextKmerList);
        
        inputNode.setAsReference(outputNode.marshalToByteArray(), 0);
        Assert.assertEquals(tempNode.toString(), inputNode.toString());
        
        int j = 5;
        for (; j < array.length - 1; j++) {
            outputNode.reset();
            curKmer.setAsCopy(nextKmer);
            
            nextKmer.shiftKmerWithNextChar(array[j+1]);
            nextKmerList.reset();
            nextKmerList.append(nextKmer);
            preKmerList.reset();
            preKmerList.append(preKmer);
            outputNode.setNodeIdList(nodeIdList);
            outputNode.setFFList(nextKmerList);
            outputNode.setRRList(preKmerList);
            tempNode.reset();
            tempNode.setNodeIdList(nodeIdList);
            tempNode.setFFList(nextKmerList);
            tempNode.setRRList(preKmerList);
            preKmer.setAsCopy(curKmer);
            inputNode.reset();
            inputNode.setAsReference(outputNode.marshalToByteArray(), 0);
            Assert.assertEquals(tempNode.toString(), inputNode.toString());
        }
        curKmer.setAsCopy(nextKmer);
        preKmerList.reset();
        preKmerList.append(preKmer);
        outputNode.reset();
        outputNode.setNodeIdList(nodeIdList);
        outputNode.setRRList(preKmerList);
        tempNode.reset();
        tempNode.setNodeIdList(nodeIdList);
        tempNode.setRRList(preKmerList);
        inputNode.reset();
        inputNode.setAsReference(outputNode.marshalToByteArray(), 0);
        Assert.assertEquals(tempNode.toString(), inputNode.toString());
        }
    }

    public String generaterRandomString(int n) {
        char[] chars = "ACGT".toCharArray();
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < n; i++) {
            char c = chars[random.nextInt(chars.length)];
            sb.append(c);
        }
        return sb.toString();
    }
}
