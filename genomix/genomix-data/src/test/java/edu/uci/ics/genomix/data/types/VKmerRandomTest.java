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
        vkmer.setFromStringBytes(actualKmerStr.getBytes(), 0);
        return vkmer;
    }
    
    @Test
    public void TestCompressKmer() {
        int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
        String input = RandomTestHelper.generateGeneString(strLength);
        VKmer vkmer = new VKmer();
        vkmer = getRandomKmer(input, strLength);
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
}
