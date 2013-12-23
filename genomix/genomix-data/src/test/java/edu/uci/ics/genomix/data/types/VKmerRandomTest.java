package edu.uci.ics.genomix.data.types;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.genomix.data.utils.GeneCode;


public class VKmerRandomTest {
    
    public static int strMaxLength;
    public static int strMinLength;

    @Before
    public void setUp() {
        strMaxLength = 9;
        strMinLength = 6;
        if ((strMinLength <= 0) || (strMaxLength <= 0)) {
            throw new IllegalArgumentException("strMinLength or strMaxLength can not be less than 0!");
        }
        if (strMinLength > strMaxLength) {
            throw new IllegalArgumentException("strMinLength can not be larger than strMaxLength!");
        }
    }
    
    public static VKmer getRandomKmer(String input, int strLength) {
        int kmerSize = RandomTestHelper.genRandomInt(1, strLength);
        String actualKmerStr = input.substring(0, kmerSize);
        VKmer vkmer = new VKmer();
        vkmer.setFromStringBytes(kmerSize, actualKmerStr.getBytes(), 0);
        return vkmer;
    }
    
    @Test
    public void TestCompressKmer() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateGeneString(strLength);
        VKmer vkmer = getRandomKmer(input, strLength);
        Assert.assertEquals(input.substring(0, vkmer.getKmerLetterLength()), vkmer.toString());
    }
    
    @Test
    public void TestConstructorFromRead() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateGeneString(strLength);
        VKmer vkmer = new VKmer(input);
        Assert.assertEquals(input.toString(), vkmer.toString());
    }
    
    @Test
    public void TestMoveKmer() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateGeneString(strLength);
        VKmer vkmer = new VKmer();
        vkmer = getRandomKmer(input, strLength);
        int kmerLength = vkmer.getKmerLetterLength();
        for (int i = kmerLength; i < strLength - 1; i++) {
            vkmer.shiftKmerWithNextChar((byte) (input.charAt(i)));
            Assert.assertEquals(input.substring(1 + i - kmerLength, 1 + i - kmerLength + kmerLength), vkmer.toString());
        }
    }
    
    @Test
    public void TestCompressKmerReverse() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateGeneString(strLength);
        int kmerSize = RandomTestHelper.genRandomInt(1, strLength);
        String actualKmerStr = input.substring(0, kmerSize);
        VKmer vkmer = new VKmer();
        vkmer.setReversedFromStringBytes(kmerSize, input.getBytes(), 0);
        Assert.assertEquals(RandomTestHelper.getFlippedGeneStr(actualKmerStr), vkmer.toString());
    }
    
    @Test
    public void TestMoveKmerReverse() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateGeneString(strLength);
        VKmer vkmer = new VKmer();
        vkmer = getRandomKmer(input, strLength);
        int kmerLength = vkmer.getKmerLetterLength();
        String expectedStr = input.substring(0, vkmer.getKmerLetterLength());
        for (int i = kmerLength; i < strLength - 1; i++) {
            expectedStr = input.charAt(i) + expectedStr;
            expectedStr = expectedStr.substring(0, expectedStr.length() - 1);
            vkmer.shiftKmerWithPreChar((byte) (input.charAt(i)));
            Assert.assertEquals(expectedStr, vkmer.toString());
        }
    }
    
    @Test
    public void TestGetGene() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateGeneString(strLength);
        VKmer vkmer = new VKmer();
        vkmer = getRandomKmer(input, strLength);
        for (int i = 0; i < vkmer.getKmerLetterLength(); i++) {
            Assert.assertEquals(input.charAt(i), (char) (GeneCode.getSymbolFromCode(vkmer.getGeneCodeAtPosition(i))));
        }
    }
    
    
    @Test
    public void TestGetOneByteFromKmer() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateGeneString(strLength);
        VKmer vkmer = getRandomKmer(input, strLength);
        String actualKmerStr = input.substring(0, vkmer.getKmerLetterLength());
        VKmer kmerAppend = new VKmer(vkmer.getKmerLetterLength());
        for (int i = 0; i < vkmer.getKmerLetterLength(); i++) {
            byte byteActual = Kmer.getOneByteFromKmerAtPosition(i, vkmer.getBlockBytes(), vkmer.getKmerOffset(), vkmer.getKmerByteLength());
            byte byteExpect = GeneCode.getCodeFromSymbol((byte) (actualKmerStr.charAt(i)));
            for (int j = 1; j < 4 && i + j < vkmer.getKmerLetterLength(); j++) {
                byteExpect += GeneCode.getCodeFromSymbol((byte) (actualKmerStr.charAt(i + j))) << (j * 2);
            }
            Assert.assertEquals(byteActual, byteExpect);
            Kmer.appendOneByteAtPosition(i, byteActual, kmerAppend.getBlockBytes(), kmerAppend.getKmerOffset(),
                    kmerAppend.getKmerByteLength());
        }
        Assert.assertEquals(vkmer.toString(), kmerAppend.toString());
    }
    
    @Test
    public void TestMergeFFKmer() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateGeneString(strLength);
        VKmer kmer1 = new VKmer();
        kmer1.setFromStringBytes(strLength - 1, input.getBytes(), 0);
        Assert.assertEquals(input.substring(0, strLength - 1), kmer1.toString());
        VKmer kmer2 = new VKmer();
        kmer2.setFromStringBytes(strLength - 1, input.getBytes(), 1);
        Assert.assertEquals(input.substring(1, strLength), kmer2.toString());
        VKmer merge = new VKmer(kmer1);
        merge.mergeWithFFKmer(strLength - 1, kmer2);
        Assert.assertEquals(input, merge.toString());
        for (int i = 1; i < strLength - 1; i++) {
            merge.setAsCopy(kmer1);
            merge.mergeWithFFKmer(i, kmer2);
            Assert.assertEquals(kmer1.toString() + kmer2.toString().substring(i - 1), merge.toString());
        }
    }
    
    @Test
    public void TestMergeFRKmer() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateGeneString(strLength);
        
        VKmer kmer1 = new VKmer();
        kmer1.setFromStringBytes(strLength - 1, input.getBytes(), 0);
        Assert.assertEquals(input.substring(0, strLength - 1), kmer1.toString());
        
        VKmer kmer2 = new VKmer();
        kmer2.setFromStringBytes(strLength - 1 , RandomTestHelper.getFlippedGeneStr(input.substring(1, strLength)).getBytes(), 0);
        Assert.assertEquals(RandomTestHelper.getFlippedGeneStr(input.substring(1, strLength)), kmer2.toString());
        
        VKmer merge = new VKmer();
        merge.setAsCopy(kmer1);
        merge.mergeWithFRKmer(strLength - 1, kmer2);
        Assert.assertEquals(input, merge.toString());
        for (int i = 1; i < strLength - 1; i++) {
            merge.setAsCopy(kmer1);
            merge.mergeWithFRKmer(i, kmer2);
            Assert.assertEquals(kmer1.toString() + input.substring(1, strLength).toString().substring(i - 1), merge.toString());
        }
        
    }
}
