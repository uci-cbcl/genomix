package edu.uci.ics.genomix.data.types;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RandomKmerFactoryTest {
    
    public static int strMaxLength;
    public static int strMinLength;
    public static int kmerDiffMax;
    public static int kmerDiffMin;
    
    @Before
    public void setUp(){
        strMaxLength = 15;
        strMinLength = 9;
        kmerDiffMax = 4;
        kmerDiffMin = 1;
        if((strMinLength >= strMaxLength) && (kmerDiffMax >= kmerDiffMin)){
            throw new IllegalStateException("incorrect test parameters!");
        }
        if((kmerDiffMax >= strMaxLength) && (kmerDiffMin <= strMinLength)){
            throw new IllegalStateException("incorrect test parameters!");
        }
    }
    
    @Test
    public void TestGetKmerByRead(){
        VKmer vkmer = new VKmer();
        int strLength = RandomDataGenHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomDataGenHelper.generateString(strLength);
        int kmerSize = strLength - RandomDataGenHelper.genRandomInt(kmerDiffMin,kmerDiffMax);
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        vkmer.setAsCopy(kmerFactory.getKmerByRead(kmerSize, input.getBytes(), 0));
        Assert.assertEquals(input.substring(0, kmerSize), vkmer.toString());
    }
    
    @Test
    public void TestGetKmerByReadReverse(){
        VKmer vkmer = new VKmer();
        int strLength = RandomDataGenHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomDataGenHelper.generateString(strLength);
        int kmerSize = strLength - RandomDataGenHelper.genRandomInt(kmerDiffMin,kmerDiffMax);
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        vkmer.setAsCopy(kmerFactory.getKmerByReadReverse(kmerSize, input.getBytes(), 0));
        String expectedReverse = RandomDataGenHelper.getReverseStr(input.substring(0, kmerSize));
        Assert.assertEquals(expectedReverse, vkmer.toString());
    }
    
    @Test
    public void TestGetLastKmeFromChainr() {
        VKmer kmer = new VKmer();
        int strLength = RandomDataGenHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomDataGenHelper.generateString(strLength);
        int kmerSize = strLength - RandomDataGenHelper.genRandomInt(kmerDiffMin,kmerDiffMax);
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
        VKmer kmer = new VKmer();
        int strLength = RandomDataGenHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomDataGenHelper.generateString(strLength);
        int kmerSize = strLength - RandomDataGenHelper.genRandomInt(kmerDiffMin,kmerDiffMax);
        kmer.setFromStringBytes(kmerSize, input.getBytes(), 0);
        Assert.assertEquals(input.substring(0, kmerSize), kmer.toString());
        VKmer firstKmer;
        KmerFactory kmerFactory = new KmerFactory(kmerSize);
        for (int i = kmerSize; i > 0; i--) {
            firstKmer = kmerFactory.getFirstKmerFromChain(i, kmer);
            Assert.assertEquals(input.substring(0, i), firstKmer.toString());
            firstKmer = kmerFactory.getSubKmerFromChain(0, i, kmer);
            Assert.assertEquals(input.substring(0, i), firstKmer.toString());
        }
    }
    
    @Test
    public void TestGetSubKmer() {
        VKmer kmer = new VKmer();
        kmer.setFromStringBytes(9, array, 0);
        Assert.assertEquals("AGCTGACCG", kmer.toString());
        VKmer subKmer;
        for (int istart = 0; istart < kmer.getKmerLetterLength() - 1; istart++) {
            for (int isize = 1; isize + istart <= kmer.getKmerLetterLength(); isize++) {
                subKmer = kmerFactory.getSubKmerFromChain(istart, isize, kmer);
                Assert.assertEquals("AGCTGACCG".substring(istart, istart + isize), subKmer.toString());
            }
        }
    }
}
