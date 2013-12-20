package edu.uci.ics.genomix.data.types;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KmerRandomTest {
    
    public static int strMaxLength;
    public static int strMinLength;
    
    @Before
    public void setUp() {
        strMaxLength = 9;
        strMinLength = 6;
        if ((strMinLength <= 0) || (strMaxLength <= 0)) {
            throw new IllegalStateException("strMinLength or strMaxLength can not be less than 0!");
        }
        if (strMinLength > strMaxLength) {
            throw new IllegalStateException("strMinLength can not be larger than strMaxLength!");
        }
    }
    
    public static Kmer getRandomKmer(int strMinLength, int strMaxLength, String input, int strLength){
        int kmerSize = RandomTestHelper.genRandomInt(1, strLength);
        String actualKmerStr = input.substring(0, kmerSize);
        Kmer.setGlobalKmerLength(kmerSize);
        Kmer kmer = new Kmer();
        kmer.setFromStringBytes(actualKmerStr.getBytes(), 0);
        return kmer;
    }
    
    @Test
    public void TestCompressKmer() throws IOException {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateGeneString(strLength);
        int kmerSize = RandomTestHelper.genRandomInt(1, strLength);
        String actualKmerStr = input.substring(0, kmerSize);
        Kmer.setGlobalKmerLength(kmerSize);
        Kmer kmer = new Kmer();
        kmer.setFromStringBytes(actualKmerStr.getBytes(), 0);
        Assert.assertEquals(actualKmerStr, kmer.toString());
    }
    
    @Test
    public void TestMoveKmer() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateGeneString(strLength);
        Kmer kmer = getRandomKmer(strMinLength, strMaxLength, input, strLength);
        int kmerLength = kmer.getKmerLength(); 
        for (int i = kmerLength; i < strLength - 1; i++) {
            kmer.shiftKmerWithNextChar((byte)(input.charAt(i)));
            Assert.assertEquals(input.substring(1 + i - kmerLength, 1 + i - kmerLength + kmerLength), kmer.toString());
        }
    }
}
