package edu.uci.ics.genomix.data.types;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.genomix.data.utils.GeneCode;

public class KmerFactoryRandomTest {
    
    public static int strMaxLength;
    public static int strMinLength;
    
    @Before
    public void setUp(){
        strMaxLength = 15;
        strMinLength = 15;
        if(strMinLength > strMaxLength){
            throw new IllegalStateException("incorrect test parameters!");
        }
    }
    
    @Test
    public void TestGetKmerByRead(){
        VKmer vkmer = new VKmer();
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateString(strLength);
        int kmerSize = RandomTestHelper.genRandomInt(1,strLength);
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        vkmer.setAsCopy(kmerFactory.getKmerByRead(kmerSize, input.getBytes(), 0));
        Assert.assertEquals(input.substring(0, kmerSize), vkmer.toString());
    }
    
    @Test
    public void TestGetKmerByReadReverse(){
        VKmer vkmer = new VKmer();
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateString(strLength);
        int kmerSize = RandomTestHelper.genRandomInt(1,strLength);
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        vkmer.setAsCopy(kmerFactory.getKmerByReadReverse(kmerSize, input.getBytes(), 0));
        String expectedReverse = RandomTestHelper.getReverseStr(input.substring(0, kmerSize));
        Assert.assertEquals(expectedReverse, vkmer.toString());
    }
    
    @Test
    public void TestGetLastKmeFromChainr() {
        VKmer kmer = new VKmer();
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateString(strLength);
        int kmerSize = RandomTestHelper.genRandomInt(1,strLength);
        kmer.setFromStringBytes(kmerSize, input.getBytes(), 0);
        Assert.assertEquals(input.substring(0, kmerSize), kmer.toString());
        VKmer lastKmer;
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        for (int i = kmerSize - 1; i > 0; i--) {
            lastKmer = kmerFactory.getLastKmerFromChain(i, kmer);
            Assert.assertEquals(input.substring(0, kmerSize).substring(kmerSize - i, kmerSize), lastKmer.toString());
            lastKmer = kmerFactory.getSubKmerFromChain(kmerSize - i, i, kmer);
            Assert.assertEquals(input.substring(0, kmerSize).substring(kmerSize - i, kmerSize), lastKmer.toString());
        }
    }
    
    @Test
    public void TestGetFirstKmerFromChain() {
        VKmer vkmer = new VKmer();
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateString(strLength);
        int kmerSize = RandomTestHelper.genRandomInt(1,strLength);
        vkmer.setFromStringBytes(kmerSize, input.getBytes(), 0);
        Assert.assertEquals(input.substring(0, kmerSize), vkmer.toString());
        VKmer firstKmer;
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        for (int i = kmerSize; i > 0; i--) {
            firstKmer = kmerFactory.getFirstKmerFromChain(i, vkmer);
            Assert.assertEquals(input.substring(0, i), firstKmer.toString());
            firstKmer = kmerFactory.getSubKmerFromChain(0, i, vkmer);
            Assert.assertEquals(input.substring(0, i), firstKmer.toString());
        }
    }
    
    @Test
    public void TestGetSubKmer() {
        VKmer vkmer = new VKmer();
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateString(strLength);
        int kmerSize = RandomTestHelper.genRandomInt(1,strLength);
        vkmer.setFromStringBytes(kmerSize, input.getBytes(), 0);
        Assert.assertEquals(input.substring(0, kmerSize), vkmer.toString());
        VKmer subKmer;
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        for (int istart = 0; istart < vkmer.getKmerLetterLength() - 1; istart++) {
            for (int isize = 1; isize + istart <= vkmer.getKmerLetterLength(); isize++) {
                subKmer = kmerFactory.getSubKmerFromChain(istart, isize, vkmer);
                Assert.assertEquals(input.substring(0, kmerSize).substring(istart, istart + isize), subKmer.toString());
            }
        }
    }
    
    @Test
    public void TestMergeNext() {
        VKmer vkmer = new VKmer();
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateString(strLength);
        int kmerSize = RandomTestHelper.genRandomInt(1,strLength);
        vkmer.setFromStringBytes(kmerSize, input.getBytes(), 0);
        Assert.assertEquals(input.substring(0, kmerSize), vkmer.toString());
        String text = input.substring(0, kmerSize);
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        for (byte x = GeneCode.A; x <= GeneCode.T; x++) {
            VKmer newkmer = kmerFactory.mergeKmerWithNextCode(vkmer, x);
            text = text + (char) GeneCode.GENE_SYMBOL[x];
            Assert.assertEquals(text, newkmer.toString());
            vkmer = new VKmer(newkmer);
        }
    }
    
    @Test
    public void TestMergePre() {
        VKmer vkmer = new VKmer();
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateString(strLength);
        int kmerSize = RandomTestHelper.genRandomInt(1,strLength);
        vkmer.setFromStringBytes(kmerSize, input.getBytes(), 0);
        Assert.assertEquals(input.substring(0, kmerSize), vkmer.toString());
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        String text = input.substring(0, kmerSize);
        for (byte x = GeneCode.A; x <= GeneCode.T; x++) {
            VKmer newkmer = kmerFactory.mergeKmerWithPreCode(vkmer, x);
            text = (char) GeneCode.GENE_SYMBOL[x] + text;
            Assert.assertEquals(text, newkmer.toString());
            vkmer = new VKmer(newkmer);
        }
    }
    
    @Test
    public void TestMergeTwoKmer() {
        VKmer vkmer1 = new VKmer();
        int strLength1 = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input1 = RandomTestHelper.generateString(strLength1);
        int kmerSize1 = RandomTestHelper.genRandomInt(1,strLength1);
        vkmer1.setFromStringBytes(kmerSize1, input1.getBytes(), 0);
        
        VKmer vkmer2 = new VKmer();
        int strLength2 = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input2 = RandomTestHelper.generateString(strLength2);
        int kmerSize2 = RandomTestHelper.genRandomInt(1,strLength2);
        vkmer2.setFromStringBytes(kmerSize2, input2.getBytes(), 0);
        
        String expected1 = input1.substring(0, kmerSize1);
        String expected2 = input2.substring(0, kmerSize2);
        Assert.assertEquals(expected1, vkmer1.toString());
        Assert.assertEquals(expected2, vkmer2.toString());
        
        KmerFactory kmerFactory = new KmerFactory(kmerSize1 + kmerSize2);
        VKmer merged = kmerFactory.mergeTwoKmer(vkmer1, vkmer2);
        StringBuilder expectedMerge = new StringBuilder();
        expectedMerge.append(expected1).append(expected2);
        Assert.assertEquals(expectedMerge.toString(), merged.toString());
        int loopTestNum = strMaxLength;
        VKmer subVkmer1 = new VKmer();
        VKmer subVkmer2 = new VKmer();
        for(int i = 0; i < loopTestNum; i++){
            int subKmer1Size = RandomTestHelper.genRandomInt(1,kmerSize1 - 1);
            int subKmer2Size = RandomTestHelper.genRandomInt(1,kmerSize2 - 1);
            subVkmer1.setAsCopy(kmerFactory.getSubKmerFromChain(0, subKmer1Size, vkmer1));
            subVkmer2.setAsCopy(kmerFactory.getSubKmerFromChain(0, subKmer2Size, vkmer2));
            VKmer subMerged = kmerFactory.mergeTwoKmer(subVkmer1, subVkmer2);
            expectedMerge.delete(0, expectedMerge.length());
            expectedMerge.append(subVkmer1.toString()).append(subVkmer2.toString());
            Assert.assertEquals(expectedMerge.toString(), subMerged.toString());
        }
    }
    
    @Test
    public void TestShiftWithNextCode() {
        
        VKmer vkmer = new VKmer();
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateString(strLength);
        int kmerSize = RandomTestHelper.genRandomInt(1,strLength);
        vkmer.setFromStringBytes(kmerSize, input.getBytes(), 0);
        String expectedKmer = input.substring(0, kmerSize);
        Assert.assertEquals(expectedKmer, vkmer.toString());
        
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        VKmer kmerForward = kmerFactory.shiftKmerWithNextCode(vkmer, GeneCode.A);
        expectedKmer = expectedKmer.substring(1) + "A";
        Assert.assertEquals(expectedKmer, kmerForward.toString());
        kmerForward = kmerFactory.shiftKmerWithNextCode(kmerForward, GeneCode.C);
        expectedKmer = expectedKmer.substring(1) + "C";
        Assert.assertEquals(expectedKmer, kmerForward.toString());
        kmerForward = kmerFactory.shiftKmerWithNextCode(kmerForward, GeneCode.G);
        expectedKmer = expectedKmer.substring(1) + "G";
        Assert.assertEquals(expectedKmer, kmerForward.toString());
        kmerForward = kmerFactory.shiftKmerWithNextCode(kmerForward, GeneCode.T);
        expectedKmer = expectedKmer.substring(1) + "T";
        Assert.assertEquals(expectedKmer, kmerForward.toString());
    }
    
    @Test
    public void TestShiftWithPreCode() {
        
        VKmer vkmer = new VKmer();
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateString(strLength);
        int kmerSize = RandomTestHelper.genRandomInt(1,strLength);
        vkmer.setFromStringBytes(kmerSize, input.getBytes(), 0);
        String expectedKmer = input.substring(0, kmerSize);
        Assert.assertEquals(expectedKmer, vkmer.toString());
        
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        VKmer kmerForward = kmerFactory.shiftKmerWithPreCode(vkmer, GeneCode.A);
        expectedKmer = "A" + expectedKmer.substring(0, kmerSize - 1);
        Assert.assertEquals(expectedKmer, kmerForward.toString());
        kmerForward = kmerFactory.shiftKmerWithPreCode(kmerForward, GeneCode.C);
        expectedKmer = "C" + expectedKmer.substring(0, kmerSize - 1);
        Assert.assertEquals(expectedKmer, kmerForward.toString());
        kmerForward = kmerFactory.shiftKmerWithPreCode(kmerForward, GeneCode.G);
        expectedKmer = "G" + expectedKmer.substring(0, kmerSize - 1);
        Assert.assertEquals(expectedKmer, kmerForward.toString());
        kmerForward = kmerFactory.shiftKmerWithPreCode(kmerForward, GeneCode.T);
        expectedKmer = "T" + expectedKmer.substring(0, kmerSize - 1);
        Assert.assertEquals(expectedKmer, kmerForward.toString());
    }

    @Test
    public void TestReverseKmer() {
        VKmer vkmer = new VKmer();
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateString(strLength);
        int kmerSize = RandomTestHelper.genRandomInt(1,strLength);
        vkmer.setFromStringBytes(kmerSize, input.getBytes(), 0);
        Assert.assertEquals(input.substring(0, kmerSize), vkmer.toString());
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        VKmer reversed = kmerFactory.reverse(vkmer);
        StringBuilder sb = new StringBuilder(input.substring(0, kmerSize));
        Assert.assertEquals(sb.reverse().toString(), reversed.toString());
    }
}
