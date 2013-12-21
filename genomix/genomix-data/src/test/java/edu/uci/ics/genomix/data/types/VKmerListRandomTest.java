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
        strMinLength = 11;
        minLoopNum = 20;
        maxLoopNum = 50;
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
            throw new IllegalArgumentException("minLoopNum can not be larger than maxLoopNum");
        }
    }
    
    public static VKmer getRandomVKmer(String input, int strLength) {
        int kmerSize = RandomTestHelper.genRandomInt(1, strLength);
        String actualKmerStr = input.substring(0, kmerSize);
        VKmer vkmer = new VKmer();
        vkmer.setFromStringBytes(actualKmerStr.getBytes(), 0);
        return vkmer;
    }
    
    @Test
    public void TestAppend() {
        VKmerList kmerList = new VKmerList();
        Assert.assertEquals(kmerList.size(), 0);
        kmerList.clear();
        int loop = RandomTestHelper.genRandomInt(minLoopNum, maxLoopNum);
        for (int i = 0; i < loop; i++) {
            int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
            String input = RandomTestHelper.generateGeneString(strLength);
            VKmer vkmer = getRandomVKmer(input, strLength);
            kmerList.append(vkmer);
            Assert.assertEquals(input.substring(0, vkmer.getKmerLetterLength()), kmerList.getPosition(i).toString());
        }
        Assert.assertEquals(loop, kmerList.size());
    }
}
