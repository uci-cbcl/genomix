package edu.uci.ics.genomix.data.types;

import org.junit.Assert;
import org.junit.Test;

public class RandomKmerFactoryTest {
    
    @Test
    public void TestGetLastKmer() {
        VKmer kmer = new VKmer();
        int strLength = RandomDataGenHelper.genRandomInt(9, 15);
        String input = RandomDataGenHelper.generateString(strLength);
        int kmerSize = strLength - RandomDataGenHelper.genRandomInt(1,4);
        kmer.setFromStringBytes(kmerSize, input.getBytes(), 0);
        Assert.assertEquals(input.substring(0, kmerSize), kmer.toString());
        VKmer lastKmer;
        KmerFactory kmerFactory = new KmerFactory(kmerSize - 1);
        for (int i = kmerSize - 1; i > 0; i--) {
            lastKmer = kmerFactory.getLastKmerFromChain(i, kmer);
            Assert.assertEquals(input.substring(0, kmerSize).substring(kmerSize - i, kmerSize), lastKmer.toString());
            lastKmer = kmerFactory.getSubKmerFromChain(kmerSize - i, i, kmer);
            Assert.assertEquals(input.substring(0, kmerSize).substring(kmerSize - i, kmerSize), lastKmer.toString());
        }
    }
}
