package edu.uci.ics.genomix.data.types;

import org.junit.Before;

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
    
    public Kmer getRandomKmer(int strMinLength, int strMaxLength, String input, int strLength){
        int kmerSize = RandomTestHelper.genRandomInt(1, strLength);
        String actualKmerStr = input.substring(0, kmerSize);
        Kmer.setGlobalKmerLength(kmerSize);
        Kmer kmer = new Kmer();
        kmer.setFromStringBytes(actualKmerStr.getBytes(), 0);
        return kmer;
    }
}
