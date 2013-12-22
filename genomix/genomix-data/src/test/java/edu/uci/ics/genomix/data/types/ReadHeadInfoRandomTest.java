package edu.uci.ics.genomix.data.types;

import java.util.Random;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.genomix.data.types.ReadHeadInfo;
import edu.uci.ics.genomix.data.utils.Marshal;

public class ReadHeadInfoRandomTest {
    
    public static int strMaxLength;
    public static int strMinLength;
    public static int maxLoopNum;
    public static int minLoopNum;
    public static int fieldMin;
    public static int fieldMax;
    
    public static final int totalBits = 64;
    private static final int bitsForMate = 1;
    private static final int bitsForLibrary = 4;
    private static final int bitsForOffset = 24;
    private static final int bitsForReadId = totalBits - bitsForOffset - bitsForLibrary - bitsForMate;
    // the offset (position) covers the leading bits, followed by the library, then mate, and finally, the readid
    // to recover each value, >>> by:
    private static final int offsetShift = bitsForLibrary + bitsForMate + bitsForReadId;
    private static final int libraryIdShift = bitsForMate + bitsForReadId;
    private static final int mateIdShift = bitsForReadId;
    private static final int readIdShift = 0;
    
    @Before
    public void setUp() {
        strMaxLength = 15;
        strMinLength = 11;
        minLoopNum = 20;
        maxLoopNum = 50;
        fieldMin = 137;
        fieldMax = 169;
        if ((strMinLength <= 0) || (strMaxLength <= 0)) {
            throw new IllegalArgumentException("strMinLength or strMaxLength can not be less than 0!");
        }
        if (strMinLength > strMaxLength) {
            throw new IllegalArgumentException("strMinLength can not be larger than strMaxLength!");
        }
        if ((maxLoopNum <= 0) || (minLoopNum <= 0)) {
            throw new IllegalArgumentException("maxLoopNum or minLoopNum can not be less than 0!");
        }
        if (minLoopNum > maxLoopNum) {
            throw new IllegalArgumentException("minLoopNum can not be larger than maxLoopNum");
        }
        if ((fieldMax <= 0) || (fieldMin <= 0)) {
            throw new IllegalArgumentException("fieldMax or fieldMin can not be less than 0!");
        }
        if (fieldMin > fieldMax) {
            throw new IllegalArgumentException("fieldMin can not be larger than fieldMax");
        }
    }
    
    public static VKmer getRandomVKmer(String input, int strLength) {
        int kmerSize = RandomTestHelper.genRandomInt(1, strLength);
        String actualKmerStr = input.substring(0, kmerSize);
        VKmer vkmer = new VKmer();
        vkmer.setFromStringBytes(actualKmerStr.getBytes(), 0);
        return vkmer;
    }
    
    public static byte getMateId(int fieldMin, int fieldMax){
        byte temp = (byte)RandomTestHelper.genRandomInt(fieldMin, fieldMax);
        return (byte) (temp & ~(-1 << (totalBits - bitsForMate)));
    }
    
    @Test
    public void testSetAndGet(){
//        int loop = RandomTestHelper.genRandomInt(minLoopNum, maxLoopNum);
//        for(int i = 0; i < loop; i++){
//            byte mateId = (byte)RandomTestHelper.genRandomInt(minLoopNum, maxLoopNum);
//            byte libraryId = (byte)RandomTestHelper.genRandomInt(minLoopNum, maxLoopNum);
//            int readId = RandomTestHelper.genRandomInt(minLoopNum, maxLoopNum);
//            int offset = RandomTestHelper.genRandomInt(minLoopNum, maxLoopNum);
//        }
        ReadHeadInfo pos = new ReadHeadInfo();
        byte mateId = 3;
        byte libraryId = 119;
        long readId = 222;
        int offset  = 5;
        pos.set(mateId, libraryId, readId, offset);
        System.out.println(~(-1 << 1));
    }
//  ReadHeadInfo pos = new ReadHeadInfo(0);
//  ReadHeadInfo pos1 = new ReadHeadInfo(0);
//  byte mateId;
//  long readId;
//  int posId;
//  Random gen = new Random();
//  byte[] start = new byte[15];
//  for (long i = 0; i < (1 << 47); i++) {
//      mateId = (byte) (gen.nextBoolean() ? 1 : 0);
//      readId = i;
//      posId = (int) (i % (1 << 16));
//      pos = new ReadHeadInfo(mateId, readId, posId);
//      Assert.assertEquals(pos.getMateId(), mateId);
//      Assert.assertEquals(pos.getReadId(), readId);
//      Assert.assertEquals(pos.getOffset(), posId);
//
//      long uuid = ((readId + 1) << 17) + ((posId & 0xFFFF) << 1) + (mateId & 0b1);
//      Marshal.putLong(uuid, start, 0);
//      pos1 = new ReadHeadInfo(uuid);
//      Assert.assertEquals(pos1.getMateId(), mateId);
//      Assert.assertEquals(pos1.getReadId(), readId + 1);
//      Assert.assertEquals(pos1.getOffset(), posId);
//
//      //Assert.assertEquals(pos1.toString(), pos.toString());
}
