package edu.uci.ics.genomix.data.types;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class VKmerListRandomTest {
    
    public static int strMaxLength;
    public static int strMinLength;
    public static int maxLoopNum;
    public static int minLoopNum;
    
    @Before
    public void setUp() {
        strMaxLength = 15;
        strMinLength = 13;
        if ((strMinLength <= 0) || (strMaxLength <= 0)) {
            throw new IllegalArgumentException("strMinLength or strMaxLength can not be less than 0!");
        }
        if (strMinLength > strMaxLength) {
            throw new IllegalArgumentException("strMinLength can not be larger than strMaxLength!");
        }
        if ((maxLoopNum <= 0) || (minLoopNum <= 0)) {
            throw new IllegalArgumentException("maxLoopNum or minLoopNum can not be less than 0!");
        }
        if(minLoopNum > maxLoopNum){
            
        }
    }
    
    @Test
    public void TestInitial() {
        VKmerList kmerList = new VKmerList();
        Assert.assertEquals(kmerList.size(), 0);
        //one kmer in list and reset each time
        VKmer kmer = new VKmer();
        kmer.setFromStringBytes(8, array, 0);
        kmerList.append(kmer);
        Assert.assertEquals("AATAGAAG", kmerList.getPosition(0).toString());
        Assert.assertEquals(1, kmerList.size());
        kmerList.clear();
        //add one more kmer each time and fix kmerSize
        for (int i = 0; i < 8; i++) {
            kmer.setFromStringBytes(i, array, 0);
            kmerList.append(kmer);
            Assert.assertEquals("AATAGAAG".substring(0, i), kmerList.getPosition(i).toString());
        }
        Assert.assertEquals(8, kmerList.size());

    }
}
