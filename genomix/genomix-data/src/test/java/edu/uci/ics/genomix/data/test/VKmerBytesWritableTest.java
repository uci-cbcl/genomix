/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.genomix.data.test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;


public class VKmerBytesWritableTest {
    static byte[] array = { 'A', 'A', 'T', 'A', 'G', 'A', 'A', 'G' };
    static int k = 7;

    @Test
    public void TestCompressKmer() {
        VKmerBytesWritable kmer = new VKmerBytesWritable(k);
        kmer.setByRead(k, array, 0);
        Assert.assertEquals(kmer.toString(), "AATAGAA");

        kmer.setByRead(k, array, 1);
        Assert.assertEquals(kmer.toString(), "ATAGAAG");
    }

    @Test
    public void TestMoveKmer() {
        VKmerBytesWritable kmer = new VKmerBytesWritable(k);
        kmer.setByRead(k, array, 0);
        Assert.assertEquals(kmer.toString(), "AATAGAA");

        for (int i = k; i < array.length - 1; i++) {
            kmer.shiftKmerWithNextCode(array[i]);
            Assert.assertTrue(false);
        }

        byte out = kmer.shiftKmerWithNextChar(array[array.length - 1]);
        Assert.assertEquals(out, GeneCode.getCodeFromSymbol((byte) 'A'));
        Assert.assertEquals(kmer.toString(), "ATAGAAG");
    }

    @Test
    public void TestCompressKmerReverse() {
        VKmerBytesWritable kmer = new VKmerBytesWritable();
        kmer.setByRead(k, array, 0);
        Assert.assertEquals(kmer.toString(), "AATAGAA");

        kmer.setByReadReverse(k, array, 1);
        Assert.assertEquals(kmer.toString(), "CTTCTAT");
    }

    @Test
    public void TestMoveKmerReverse() {
        VKmerBytesWritable kmer = new VKmerBytesWritable();
        kmer.setByRead(k, array, 0);
        Assert.assertEquals(kmer.toString(), "AATAGAA");

        for (int i = k; i < array.length - 1; i++) {
            kmer.shiftKmerWithPreChar(array[i]);
            Assert.assertTrue(false);
        }

        byte out = kmer.shiftKmerWithPreChar(array[array.length - 1]);
        Assert.assertEquals(out, GeneCode.getCodeFromSymbol((byte) 'A'));
        Assert.assertEquals(kmer.toString(), "GAATAGA");
    }

    @Test
    public void TestGetGene() {
        VKmerBytesWritable kmer = new VKmerBytesWritable();
        String text = "AGCTGACCG";
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G' };
        kmer.setByRead(9, array, 0);

        for (int i = 0; i < 9; i++) {
            Assert.assertEquals(text.charAt(i), (char) (GeneCode.getSymbolFromCode(kmer.getGeneCodeAtPosition(i))));
        }
    }

    @Test
    public void TestGetOneByteFromKmer() {
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };
        String string = "AGCTGACCGT";
        for (int k = 3; k <= 10; k++) {
            VKmerBytesWritable kmer = new VKmerBytesWritable();
            VKmerBytesWritable kmerAppend = new VKmerBytesWritable();
            kmer.setByRead(k, array, 0);
            Assert.assertEquals(string.substring(0, k), kmer.toString());
            for (int b = 0; b < k; b++) {
                byte byteActual = KmerBytesWritable.getOneByteFromKmerAtPosition(b, kmer.getBytes(),
                        kmer.getKmerOffset(), kmer.getKmerByteLength());
                byte byteExpect = GeneCode.getCodeFromSymbol(array[b]);
                for (int i = 1; i < 4 && b + i < k; i++) {
                    byteExpect += GeneCode.getCodeFromSymbol(array[b + i]) << (i * 2);
                }
                Assert.assertEquals(byteActual, byteExpect);
                KmerBytesWritable.appendOneByteAtPosition(b, byteActual, kmerAppend.getBytes(),
                        kmerAppend.getKmerOffset(), kmerAppend.getKmerByteLength());
            }
            Assert.assertEquals(kmer.toString(), kmerAppend.toString());
        }
    }

    @Test
    public void TestMergeFFKmer() {
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };
        String text = "AGCTGACCGT";
        VKmerBytesWritable kmer1 = new VKmerBytesWritable();
        kmer1.setByRead(8, array, 0);
        String text1 = "AGCTGACC";
        Assert.assertEquals(text1, kmer1.toString());

        VKmerBytesWritable kmer2 = new VKmerBytesWritable();
        kmer2.setByRead(8, array, 1);
        String text2 = "GCTGACCG";
        Assert.assertEquals(text2, kmer2.toString());

        VKmerBytesWritable merge = new VKmerBytesWritable(kmer1);
        int kmerSize = 8;
        merge.mergeWithFFKmer(kmerSize, kmer2);
        Assert.assertEquals(text1 + text2.substring(kmerSize - 1), merge.toString());

        for (int i = 1; i < 8; i++) {
            merge.setAsCopy(kmer1);
            merge.mergeWithFFKmer(i, kmer2);
            Assert.assertEquals(text1 + text2.substring(i - 1), merge.toString());
        }

        for (int ik = 1; ik <= 10; ik++) {
            for (int jk = 1; jk <= 10; jk++) {
                kmer1 = new VKmerBytesWritable(ik);
                kmer2 = new VKmerBytesWritable(jk);
                kmer1.setByRead(ik, array, 0);
                kmer2.setByRead(jk, array, 0);
                text1 = text.substring(0, ik);
                text2 = text.substring(0, jk);
                Assert.assertEquals(text1, kmer1.toString());
                Assert.assertEquals(text2, kmer2.toString());
                for (int x = 1; x < jk; x++) {
                    merge.setAsCopy(kmer1);
                    merge.mergeWithFFKmer(x, kmer2);
                    Assert.assertEquals(text1 + text2.substring(x - 1), merge.toString());
                }
            }
        }
    }

    @Test
    public void TestMergeFRKmer() {
        int kmerSize = 3;
        String result = "AAGCTAACAACC";
        byte[] resultArray = result.getBytes();

        String text1 = "AAGCTAA";
        VKmerBytesWritable kmer1 = new VKmerBytesWritable();
        kmer1.setByRead(text1.length(), resultArray, 0);
        Assert.assertEquals(text1, kmer1.toString());

        // kmer2 is the rc of the end of the read
        String text2 = "GGTTGTT";
        VKmerBytesWritable kmer2 = new VKmerBytesWritable();
        kmer2.setByReadReverse(text2.length(), resultArray, result.length() - text2.length());
        Assert.assertEquals(text2, kmer2.toString());

        VKmerBytesWritable merge = new VKmerBytesWritable();
        merge.mergeWithFRKmer(kmerSize, kmer2);
        Assert.assertEquals(result, merge.toString());

        int i = 1;
        merge.setAsCopy(kmer1);
        merge.mergeWithFRKmer(i, kmer2);
        Assert.assertEquals("AAGCTAAAACAACC", merge.toString());

        i = 2;
        merge.setAsCopy(kmer1);
        merge.mergeWithFRKmer(i, kmer2);
        Assert.assertEquals("AAGCTAAACAACC", merge.toString());

        i = 3;
        merge.setAsCopy(kmer1);
        merge.mergeWithFRKmer(i, kmer2);
        Assert.assertEquals("AAGCTAACAACC", merge.toString());
    }

    @Test
    public void TestMergeRFKmer() {
        int kmerSize = 3;
        String result = "GGCACAACAACCC";
        byte[] resultArray = result.getBytes();

        String text1 = "AACAACCC";
        VKmerBytesWritable kmer1 = new VKmerBytesWritable();
        kmer1.setByRead(text1.length(), resultArray, 5);
        Assert.assertEquals(text1, kmer1.toString());

        // kmer2 is the rc of the end of the read
        String text2 = "TTGTGCC";
        VKmerBytesWritable kmer2 = new VKmerBytesWritable();
        kmer2.setByReadReverse(text2.length(), resultArray, 0);
        Assert.assertEquals(text2, kmer2.toString());

        VKmerBytesWritable merge = new VKmerBytesWritable();
        merge.mergeWithRFKmer(kmerSize, kmer2);
        Assert.assertEquals(result, merge.toString());

        int i = 1;
        merge.setAsCopy(kmer1);
        merge.mergeWithRFKmer(i, kmer2);
        Assert.assertEquals("GGCACAAAACAACCC", merge.toString());

        i = 2;
        merge.setAsCopy(kmer1);
        merge.mergeWithRFKmer(i, kmer2);
        Assert.assertEquals("GGCACAAACAACCC", merge.toString());

        i = 3;
        merge.setAsCopy(kmer1);
        merge.mergeWithRFKmer(i, kmer2);
        Assert.assertEquals("GGCACAACAACCC", merge.toString());

        // String test1 = "CTTAT";
        // String test2 = "AGACC"; // rc = GGTCT
        // VKmerBytesWritable k1 = new VKmerBytesWritable(5);
        // VKmerBytesWritable k2 = new VKmerBytesWritable(5);
        // k1.setByRead(test1.getBytes(), 0);
        // k2.setByRead(test2.getBytes(), 0);
        // k1.mergeWithRFKmer(3, k2);
        // Assert.assertEquals("GGTCTTAT", k1.toString()); //GGTCGTCT ->
        // AGACGACC ??

        String test3 = "CTA";
        String test4 = "AGA"; // rc = TCT
        VKmerBytesWritable k3 = new VKmerBytesWritable();
        VKmerBytesWritable k4 = new VKmerBytesWritable();
        k3.setByRead(3, test3.getBytes(), 0);
        k4.setByRead(3, test4.getBytes(), 0);
        k3.mergeWithRFKmer(3, k4);
        Assert.assertEquals("TCTA", k3.toString());
        // Assert.assertEquals("CTAT", k3); // this is an incorrect test case--
        // the merge always flips the passed-in kmer
        
        String test1;
        String test2;
        test1 = "CTA";
        test2 = "AGA";
        VKmerBytesWritable k1 = new VKmerBytesWritable();
        VKmerBytesWritable k2 = new VKmerBytesWritable();
        k1.setByRead(3, test1.getBytes(), 0);
        k2.setByRead(3, test2.getBytes(), 0);
        k1.mergeWithRFKmer(3, k2);
        Assert.assertEquals("TCTA", k1.toString());
        
        
        
        test1 = "CTA";
        test2 = "ATA"; //TAT
        k1 = new VKmerBytesWritable();
        k2 = new VKmerBytesWritable();
        k1.setByRead(3, test1.getBytes(), 0);
        k2.setByRead(3, test2.getBytes(), 0);
        k1.mergeWithFRKmer(3, k2);
        Assert.assertEquals("CTAT", k1.toString());
        
        test1 = "ATA";
        test2 = "CTA"; //TAT
        k1 = new VKmerBytesWritable();
        k2 = new VKmerBytesWritable();
        k1.setByRead(3, test1.getBytes(), 0);
        k2.setByRead(3, test2.getBytes(), 0);
        k1.mergeWithFRKmer(3, k2);
        Assert.assertEquals("ATAG", k1.toString());
        
        test1 = "TCTAT";
        test2 = "GAAC";
        k1 = new VKmerBytesWritable();
        k2 = new VKmerBytesWritable();
        k1.setByRead(5, test1.getBytes(), 0);
        k2.setByRead(4, test2.getBytes(), 0);
        k1.mergeWithRFKmer(3, k2);
        Assert.assertEquals("GTTCTAT", k1.toString());
    }

    @Test
    public void TestMergeRRKmer() {
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };
        String text = "AGCTGACCGT";
        VKmerBytesWritable kmer1 = new VKmerBytesWritable();
        kmer1.setByRead(8, array, 0);
        String text1 = "AGCTGACC";
        VKmerBytesWritable kmer2 = new VKmerBytesWritable();
        kmer2.setByRead(8, array, 1);
        String text2 = "GCTGACCG";
        Assert.assertEquals(text2, kmer2.toString());
        VKmerBytesWritable merge = new VKmerBytesWritable(kmer2);
        int kmerSize = 8;
        merge.mergeWithRRKmer(kmerSize, kmer1);
        Assert.assertEquals(text1 + text2.substring(kmerSize - 1), merge.toString());

        for (int i = 1; i < 8; i++) {
            merge.setAsCopy(kmer2);
            merge.mergeWithRRKmer(i, kmer1);
            Assert.assertEquals(text1.substring(0, text1.length() - i + 1) + text2, merge.toString());
        }

        for (int ik = 1; ik <= 10; ik++) {
            for (int jk = 1; jk <= 10; jk++) {
                kmer1 = new VKmerBytesWritable();
                kmer2 = new VKmerBytesWritable();
                kmer1.setByRead(ik, array, 0);
                kmer2.setByRead(jk, array, 0);
                text1 = text.substring(0, ik);
                text2 = text.substring(0, jk);
                Assert.assertEquals(text1, kmer1.toString());
                Assert.assertEquals(text2, kmer2.toString());
                for (int x = 1; x < ik; x++) {
                    merge.setAsCopy(kmer2);
                    merge.mergeWithRRKmer(x, kmer1);
                    Assert.assertEquals(text1.substring(0, text1.length() - x + 1) + text2, merge.toString());
                }
            }
        }
    }

    @Test
    public void TestMergeRFAndRRKmer() {
        String test1 = "TAGAT";
        String test2 = "TCTAG"; // rc = CTAGA
        String test3 = "GCTAG";
        VKmerBytesWritable k1 = new VKmerBytesWritable();
        VKmerBytesWritable k2 = new VKmerBytesWritable();
        VKmerBytesWritable k3 = new VKmerBytesWritable();
        k1.setByRead(5, test1.getBytes(), 0);
        k2.setByRead(5, test2.getBytes(), 0);
        k3.setByRead(5, test3.getBytes(), 0);
        k1.mergeWithRFKmer(5, k2);
        Assert.assertEquals("CTAGAT", k1.toString());
        k1.mergeWithRRKmer(5, k3);
        Assert.assertEquals("GCTAGAT", k1.toString());
    }

    @Test
    public void TestMergeRFAndRFKmer() {
        String test1 = "TAGAT";
        String test2 = "TCTAG"; // rc = CTAGA
        String test3 = "CTAGC"; // rc = GCTAG
        VKmerBytesWritable k1 = new VKmerBytesWritable();
        VKmerBytesWritable k2 = new VKmerBytesWritable();
        VKmerBytesWritable k3 = new VKmerBytesWritable();
        k1.setByRead(5, test1.getBytes(), 0);
        k2.setByRead(5, test2.getBytes(), 0);
        k3.setByRead(5, test3.getBytes(), 0);
        k1.mergeWithRFKmer(5, k2);
        Assert.assertEquals("CTAGAT", k1.toString());
        k1.mergeWithRFKmer(5, k3);
        Assert.assertEquals("GCTAGAT", k1.toString());
    }

    @Test
    public void TestMergeRFAndFRKmer() {
        String test1 = "TAGAT"; // rc = ATCTA
        String test2 = "TCTAG"; // rc = CTAGA
        String test3 = "GCTAG"; // rc = CTAGC
        VKmerBytesWritable k1 = new VKmerBytesWritable();
        VKmerBytesWritable k2 = new VKmerBytesWritable();
        VKmerBytesWritable k3 = new VKmerBytesWritable();
        k1.setByRead(5, test1.getBytes(), 0);
        k2.setByRead(5, test2.getBytes(), 0);
        k3.setByRead(5, test3.getBytes(), 0);
        k2.mergeWithRFKmer(5, k1);
        Assert.assertEquals("ATCTAG", k2.toString());
        k2.mergeWithFRKmer(5, k3);
        Assert.assertEquals("ATCTAGC", k2.toString());
    }

    @Test
    public void TestMergeRFAndFFKmer() {
        String test1 = "TAGAT"; // rc = ATCTA
        String test2 = "TCTAG"; // rc = CTAGA
        String test3 = "CTAGC"; // rc = GCTAG
        VKmerBytesWritable k1 = new VKmerBytesWritable();
        VKmerBytesWritable k2 = new VKmerBytesWritable();
        VKmerBytesWritable k3 = new VKmerBytesWritable();
        k1.setByRead(5, test1.getBytes(), 0);
        k2.setByRead(5, test2.getBytes(), 0);
        k3.setByRead(5, test3.getBytes(), 0);
        k2.mergeWithRFKmer(5, k1);
        Assert.assertEquals("ATCTAG", k2.toString());
        k2.mergeWithFFKmer(5, k3);
        Assert.assertEquals("ATCTAGC", k2.toString());
    }

    @Test
    public void TestMergeThreeVKmersRF_FF() {
        String test1 = "TAGAT"; // rc = ATCTA
        String test2 = "TCTAG"; // rc = CTAGA
        String test3 = "CTAGC"; // rc = GCTAG
        VKmerBytesWritable k1 = new VKmerBytesWritable();
        VKmerBytesWritable k2 = new VKmerBytesWritable();
        VKmerBytesWritable k3 = new VKmerBytesWritable();
        k1.setByRead(5, test1.getBytes(), 0);
        k2.setByRead(5, test2.getBytes(), 0);
        k3.setByRead(5, test3.getBytes(), 0);
        k2.mergeWithRFKmer(5, k1);
        Assert.assertEquals("ATCTAG", k2.toString());
        k2.mergeWithFFKmer(5, k3);
        Assert.assertEquals("ATCTAGC", k2.toString());
    }

    @Test
    public void TestMergeThreeVKmerRF_RF() {
        String test1 = "TAGAT";
        String test2 = "TCTAG"; // rc = CTAGA
        String test3 = "CTAGC"; // rc = GCTAG
        VKmerBytesWritable k1 = new VKmerBytesWritable();
        VKmerBytesWritable k2 = new VKmerBytesWritable();
        VKmerBytesWritable k3 = new VKmerBytesWritable();
        k1.setByRead(5, test1.getBytes(), 0);
        k2.setByRead(5, test2.getBytes(), 0);
        k3.setByRead(5, test3.getBytes(), 0);
        k1.mergeWithRFKmer(5, k2);
        Assert.assertEquals("CTAGAT", k1.toString());
        k1.mergeWithRFKmer(5, k3);
        Assert.assertEquals("GCTAGAT", k1.toString());
    }
    
    @Test
    public void TestFinalMerge() {
        String selfString;
        String match;
        String msgString;
        int index;
        VKmerBytesWritable kmer = new VKmerBytesWritable();
        int kmerSize = 3;
        
        String F1 = "AATAG";
        String F2 = "TAGAA";
        String R1 = "CTATT";
        String R2 = "TTCTA";
        
        //FF test
        selfString = F1;
        match = selfString.substring(selfString.length() - kmerSize + 1,selfString.length()); 
        msgString = F2;
        index = msgString.indexOf(match);
        // does this test belong in VKmer so it can have variable-length kmers?
//        kmer.reset(msgString.length() - index);
        kmer.setByRead(kmerSize, msgString.substring(index).getBytes(), 0);
        System.out.println(kmer.toString());
        
        //FR test
        selfString = F1;
        match = selfString.substring(selfString.length() - kmerSize + 1,selfString.length()); 
        msgString = GeneCode.reverseComplement(R2);
        index = msgString.indexOf(match);
        kmer.reset(msgString.length() - index);
        kmer.setByRead(kmerSize, msgString.substring(index).getBytes(), 0);
        System.out.println(kmer.toString());
        
        //RF test
        selfString = R1;
        match = selfString.substring(0,kmerSize - 1); 
        msgString = GeneCode.reverseComplement(F2);
        index = msgString.lastIndexOf(match) + kmerSize - 2;
        kmer.reset(index + 1);
        kmer.setByReadReverse(kmerSize, msgString.substring(0, index + 1).getBytes(), 0);
        System.out.println(kmer.toString());
        
        //RR test
        selfString = R1;
        match = selfString.substring(0,kmerSize - 1); 
        msgString = R2;
        index = msgString.lastIndexOf(match) + kmerSize - 2;
        kmer.reset(index + 1);
        kmer.setByRead(kmerSize, msgString.substring(0, index + 1).getBytes(), 0);
        System.out.println(kmer.toString());
        
        String[][] connectedTable = new String[][]{
                {"FF", "RF"},
                {"FF", "RR"},
                {"FR", "RF"},
                {"FR", "RR"}
        };
        System.out.println(connectedTable[0][1]);
        
        Set<Long> s1 = new HashSet<Long>();
        Set<Long> s2 = new HashSet<Long>();
        s1.add((long) 1);
        s1.add((long) 2);
        s2.add((long) 2);
        s2.add((long) 3);
        Set<Long> intersection = new HashSet<Long>();
        intersection.addAll(s1);
        intersection.retainAll(s2);
        System.out.println(intersection.toString());
        Set<Long> difference = new HashSet<Long>();
        difference.addAll(s1);
        difference.removeAll(s2);
        System.out.println(difference.toString());
        
        Map<VKmerBytesWritable, Set<Long>> map = new HashMap<VKmerBytesWritable, Set<Long>>();
        VKmerBytesWritable k1 = new VKmerBytesWritable();
        Set<Long> set1 = new HashSet<Long>();
        k1.setByRead(3, ("CTA").getBytes(), 0);
        set1.add((long)1);
        map.put(k1, set1);
        VKmerBytesWritable k2 = new VKmerBytesWritable();
        k2.setByRead(3, ("GTA").getBytes(), 0);
        Set<Long> set2 = new HashSet<Long>();
        set2.add((long) 2);
        map.put(k2, set2);
        VKmerBytesWritable k3 = new VKmerBytesWritable();
        k3.setByRead(3, ("ATG").getBytes(), 0);
        Set<Long> set3 = new HashSet<Long>();
        set3.add((long) 2);
        map.put(k3, set3);
        VKmerBytesWritable k4 = new VKmerBytesWritable();
        k4.setByRead(3, ("AAT").getBytes(), 0);
        Set<Long> set4 = new HashSet<Long>();
        set4.add((long) 1);
        map.put(k4, set4);
        VKmerListWritable kmerList = new VKmerListWritable();
        kmerList.append(k1);
        kmerList.append(k2);
        System.out.println("CTA = " + map.get(k1).toString());
        System.out.println("GTA = " + map.get(k2).toString());
        System.out.println("ATG = " + map.get(k3).toString());
        System.out.println("AAT = " + map.get(k4).toString());
        System.out.println(k1.compareTo(k2));
        System.out.println(k2.compareTo(k1));
        
        System.out.println("CTA = " + kmerList.getPosition(0).toString());
        System.out.println("GTA = " + kmerList.getPosition(1).toString());
        System.out.println("CTA = " + map.get(kmerList.getPosition(0)).toString());
        System.out.println("GTA = " + map.get(kmerList.getPosition(1)).toString());
    }

}
