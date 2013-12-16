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

package edu.uci.ics.genomix.data.types;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;
import edu.uci.ics.genomix.data.utils.GeneCode;

public class VKmerTest {
    static byte[] array = { 'A', 'A', 'T', 'A', 'G', 'A', 'A', 'G' };
    static int k = 7;

    @Test
    public void TestCompressKmer() {
        VKmer kmer = new VKmer(k);
        kmer.setFromStringBytes(k, array, 0);
        Assert.assertEquals(kmer.toString(), "AATAGAA");

        kmer.setFromStringBytes(k, array, 1);
        Assert.assertEquals(kmer.toString(), "ATAGAAG");
    }

    @Test
    public void TestConstructorFromRead() {
        String kmerStr = "ACTAGCTAGCTAGTCGATCGACTAGCTGATCGATCGATCGTAGCTAGC";
        VKmer kmer = new VKmer(kmerStr);
        Assert.assertEquals(kmerStr.length(), kmer.getKmerLetterLength());
        Assert.assertEquals(kmerStr.toString(), kmer.toString());
    }

    @Test
    public void TestMoveKmer() {
        VKmer kmer = new VKmer(k);
        kmer.setFromStringBytes(k, array, 0);
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
        VKmer kmer = new VKmer();
        kmer.setFromStringBytes(k, array, 0);
        Assert.assertEquals(kmer.toString(), "AATAGAA");

        kmer.setReversedFromStringBytes(k, array, 1);
        Assert.assertEquals(kmer.toString(), "CTTCTAT");
    }

    @Test
    public void TestMoveKmerReverse() {
        VKmer kmer = new VKmer();
        kmer.setFromStringBytes(k, array, 0);
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
        VKmer kmer = new VKmer();
        String text = "AGCTGACCG";
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G' };
        kmer.setFromStringBytes(9, array, 0);

        for (int i = 0; i < 9; i++) {
            Assert.assertEquals(text.charAt(i), (char) (GeneCode.getSymbolFromCode(kmer.getGeneCodeAtPosition(i))));
        }
    }

    @Test
    public void TestGetOneByteFromKmer() {
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };
        String string = "AGCTGACCGT";
        for (int k = 3; k <= 10; k++) {
            VKmer kmer = new VKmer();
            VKmer kmerAppend = new VKmer(k);
            kmer.setFromStringBytes(k, array, 0);
            Assert.assertEquals(string.substring(0, k), kmer.toString());
            for (int b = 0; b < k; b++) {
                byte byteActual = Kmer.getOneByteFromKmerAtPosition(b, kmer.getBlockBytes(), kmer.getKmerOffset(),
                        kmer.getKmerByteLength());
                byte byteExpect = GeneCode.getCodeFromSymbol(array[b]);
                for (int i = 1; i < 4 && b + i < k; i++) {
                    byteExpect += GeneCode.getCodeFromSymbol(array[b + i]) << (i * 2);
                }
                Assert.assertEquals(byteActual, byteExpect);
                Kmer.appendOneByteAtPosition(b, byteActual, kmerAppend.getBlockBytes(), kmerAppend.getKmerOffset(),
                        kmerAppend.getKmerByteLength());
            }
            Assert.assertEquals(kmer.toString(), kmerAppend.toString());
        }
    }

    @Test
    public void TestMergeFFKmer() {
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };
        String text = "AGCTGACCGT";
        VKmer kmer1 = new VKmer();
        kmer1.setFromStringBytes(8, array, 0);
        String text1 = "AGCTGACC";
        Assert.assertEquals(text1, kmer1.toString());

        VKmer kmer2 = new VKmer();
        kmer2.setFromStringBytes(8, array, 1);
        String text2 = "GCTGACCG";
        Assert.assertEquals(text2, kmer2.toString());

        VKmer merge = new VKmer(kmer1);
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
                kmer1 = new VKmer(ik);
                kmer2 = new VKmer(jk);
                kmer1.setFromStringBytes(ik, array, 0);
                kmer2.setFromStringBytes(jk, array, 0);
                text1 = text.substring(0, ik);
                text2 = text.substring(0, jk);
                Assert.assertEquals(text1, kmer1.toString());
                Assert.assertEquals(text2, kmer2.toString());
                for (int x = 1; x < (jk < ik ? jk : ik); x++) {
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
        VKmer kmer1 = new VKmer();
        kmer1.setFromStringBytes(text1.length(), resultArray, 0);
        Assert.assertEquals(text1, kmer1.toString());

        // kmer2 is the rc of the end of the read
        String text2 = "GGTTGTT";
        VKmer kmer2 = new VKmer();
        kmer2.setReversedFromStringBytes(text2.length(), resultArray, result.length() - text2.length());
        Assert.assertEquals(text2, kmer2.toString());

        VKmer merge = new VKmer();
        merge.setAsCopy(kmer1);
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
        VKmer kmer1 = new VKmer();
        kmer1.setFromStringBytes(text1.length(), resultArray, 5);
        Assert.assertEquals(text1, kmer1.toString());

        // kmer2 is the rc of the end of the read
        String text2 = "TTGTGCC";
        VKmer kmer2 = new VKmer();
        kmer2.setReversedFromStringBytes(text2.length(), resultArray, 0);
        Assert.assertEquals(text2, kmer2.toString());

        VKmer merge = new VKmer();
        merge.setAsCopy(kmer1);
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
        VKmer k3 = new VKmer();
        VKmer k4 = new VKmer();
        k3.setFromStringBytes(3, test3.getBytes(), 0);
        k4.setFromStringBytes(3, test4.getBytes(), 0);
        k3.mergeWithRFKmer(3, k4);
        Assert.assertEquals("TCTA", k3.toString());
        // Assert.assertEquals("CTAT", k3); // this is an incorrect test case--
        // the merge always flips the passed-in kmer

        String test1;
        String test2;
        test1 = "CTA";
        test2 = "AGA";
        VKmer k1 = new VKmer();
        VKmer k2 = new VKmer();
        k1.setFromStringBytes(3, test1.getBytes(), 0);
        k2.setFromStringBytes(3, test2.getBytes(), 0);
        k1.mergeWithRFKmer(3, k2);
        Assert.assertEquals("TCTA", k1.toString());

        test1 = "CTA";
        test2 = "ATA"; //TAT
        k1 = new VKmer();
        k2 = new VKmer();
        k1.setFromStringBytes(3, test1.getBytes(), 0);
        k2.setFromStringBytes(3, test2.getBytes(), 0);
        k1.mergeWithFRKmer(3, k2);
        Assert.assertEquals("CTAT", k1.toString());

        test1 = "ATA";
        test2 = "CTA"; //TAT
        k1 = new VKmer();
        k2 = new VKmer();
        k1.setFromStringBytes(3, test1.getBytes(), 0);
        k2.setFromStringBytes(3, test2.getBytes(), 0);
        k1.mergeWithFRKmer(3, k2);
        Assert.assertEquals("ATAG", k1.toString());

        test1 = "TCTAT";
        test2 = "GAAC";
        k1 = new VKmer();
        k2 = new VKmer();
        k1.setFromStringBytes(5, test1.getBytes(), 0);
        k2.setFromStringBytes(4, test2.getBytes(), 0);
        k1.mergeWithRFKmer(3, k2);
        Assert.assertEquals("GTTCTAT", k1.toString());
    }

    @Test
    public void TestMergeRRKmer() {
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };
        String text = "AGCTGACCGT";
        VKmer kmer1 = new VKmer();
        kmer1.setFromStringBytes(8, array, 0);
        String text1 = "AGCTGACC";
        VKmer kmer2 = new VKmer();
        kmer2.setFromStringBytes(8, array, 1);
        String text2 = "GCTGACCG";
        Assert.assertEquals(text2, kmer2.toString());
        VKmer merge = new VKmer(kmer2);
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
                kmer1 = new VKmer();
                kmer2 = new VKmer();
                kmer1.setFromStringBytes(ik, array, 0);
                kmer2.setFromStringBytes(jk, array, 0);
                text1 = text.substring(0, ik);
                text2 = text.substring(0, jk);
                Assert.assertEquals(text1, kmer1.toString());
                Assert.assertEquals(text2, kmer2.toString());
                for (int x = 1; x < (ik < jk ? ik : jk); x++) {
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
        VKmer k1 = new VKmer();
        VKmer k2 = new VKmer();
        VKmer k3 = new VKmer();
        k1.setFromStringBytes(5, test1.getBytes(), 0);
        k2.setFromStringBytes(5, test2.getBytes(), 0);
        k3.setFromStringBytes(5, test3.getBytes(), 0);
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
        VKmer k1 = new VKmer();
        VKmer k2 = new VKmer();
        VKmer k3 = new VKmer();
        k1.setFromStringBytes(5, test1.getBytes(), 0);
        k2.setFromStringBytes(5, test2.getBytes(), 0);
        k3.setFromStringBytes(5, test3.getBytes(), 0);
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
        VKmer k1 = new VKmer();
        VKmer k2 = new VKmer();
        VKmer k3 = new VKmer();
        k1.setFromStringBytes(5, test1.getBytes(), 0);
        k2.setFromStringBytes(5, test2.getBytes(), 0);
        k3.setFromStringBytes(5, test3.getBytes(), 0);
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
        VKmer k1 = new VKmer();
        VKmer k2 = new VKmer();
        VKmer k3 = new VKmer();
        k1.setFromStringBytes(5, test1.getBytes(), 0);
        k2.setFromStringBytes(5, test2.getBytes(), 0);
        k3.setFromStringBytes(5, test3.getBytes(), 0);
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
        VKmer k1 = new VKmer();
        VKmer k2 = new VKmer();
        VKmer k3 = new VKmer();
        k1.setFromStringBytes(5, test1.getBytes(), 0);
        k2.setFromStringBytes(5, test2.getBytes(), 0);
        k3.setFromStringBytes(5, test3.getBytes(), 0);
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
        VKmer k1 = new VKmer();
        VKmer k2 = new VKmer();
        VKmer k3 = new VKmer();
        k1.setFromStringBytes(5, test1.getBytes(), 0);
        k2.setFromStringBytes(5, test2.getBytes(), 0);
        k3.setFromStringBytes(5, test3.getBytes(), 0);
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
        VKmer kmer = new VKmer();
        int kmerSize = 3;

        String F1 = "AATAG";
        String F2 = "TAGAA";
        String R1 = "CTATT";
        String R2 = "TTCTA";

        //FF test
        selfString = F1;
        match = selfString.substring(selfString.length() - kmerSize + 1, selfString.length());
        msgString = F2;
        index = msgString.indexOf(match);
        // does this test belong in VKmer so it can have variable-length kmers?
        //        kmer.reset(msgString.length() - index);
        kmer.setFromStringBytes(kmerSize, msgString.substring(index).getBytes(), 0);
        Assert.assertEquals("AGA", kmer.toString());

        //FR test
        selfString = F1;
        match = selfString.substring(selfString.length() - kmerSize + 1, selfString.length());
        msgString = GeneCode.reverseComplement(R2);
        index = msgString.indexOf(match);
        kmer.reset(msgString.length() - index);
        kmer.setFromStringBytes(kmerSize, msgString.substring(index).getBytes(), 0);
        Assert.assertEquals("AGA", kmer.toString());

        //RF test
        selfString = R1;
        match = selfString.substring(0, kmerSize - 1);
        msgString = GeneCode.reverseComplement(F2);
        index = msgString.lastIndexOf(match) + kmerSize - 2;
        kmer.reset(index + 1);
        kmer.setReversedFromStringBytes(kmerSize, msgString.substring(0, index + 1).getBytes(), 0);
        Assert.assertEquals("GAA", kmer.toString());

        //RR test
        selfString = R1;
        match = selfString.substring(0, kmerSize - 1);
        msgString = R2;
        index = msgString.lastIndexOf(match) + kmerSize - 2;
        kmer.reset(index + 1);
        kmer.setFromStringBytes(kmerSize, msgString.substring(0, index + 1).getBytes(), 0);
        Assert.assertEquals("TTC", kmer.toString());

        String[][] connectedTable = new String[][] { { "FF", "RF" }, { "FF", "RR" }, { "FR", "RF" }, { "FR", "RR" } };
        Assert.assertEquals("RF", connectedTable[0][1]);

        Set<Long> s1 = new HashSet<Long>();
        Set<Long> s2 = new HashSet<Long>();
        s1.add((long) 1);
        s1.add((long) 2);
        s2.add((long) 2);
        s2.add((long) 3);
        Set<Long> intersection = new HashSet<Long>();
        intersection.addAll(s1);
        intersection.retainAll(s2);
        Assert.assertEquals("[2]", intersection.toString());
        Set<Long> difference = new HashSet<Long>();
        difference.addAll(s1);
        difference.removeAll(s2);
        Assert.assertEquals("[1]", difference.toString());

        Map<VKmer, Set<Long>> map = new HashMap<VKmer, Set<Long>>();
        VKmer k1 = new VKmer();
        Set<Long> set1 = new HashSet<Long>();
        k1.setFromStringBytes(3, ("CTA").getBytes(), 0);
        set1.add((long) 1);
        map.put(k1, set1);
        VKmer k2 = new VKmer();
        k2.setFromStringBytes(3, ("GTA").getBytes(), 0);
        Set<Long> set2 = new HashSet<Long>();
        set2.add((long) 2);
        map.put(k2, set2);
        VKmer k3 = new VKmer();
        k3.setFromStringBytes(3, ("ATG").getBytes(), 0);
        Set<Long> set3 = new HashSet<Long>();
        set3.add((long) 2);
        map.put(k3, set3);
        VKmer k4 = new VKmer();
        k4.setFromStringBytes(3, ("AAT").getBytes(), 0);
        Set<Long> set4 = new HashSet<Long>();
        set4.add((long) 1);
        map.put(k4, set4);
        VKmerList kmerList = new VKmerList();
        kmerList.append(k1);
        kmerList.append(k2);
        Assert.assertEquals("[1]", map.get(k1).toString());
        Assert.assertEquals("[2]", map.get(k2).toString());
        Assert.assertEquals("[2]", map.get(k3).toString());
        Assert.assertEquals("[1]", map.get(k4).toString());
        Assert.assertEquals(-1, k1.compareTo(k2));
        Assert.assertEquals(1, k2.compareTo(k1));
        Assert.assertEquals("CTA", kmerList.getPosition(0).toString());
        Assert.assertEquals("GTA", kmerList.getPosition(1).toString());
        Assert.assertEquals("[1]", map.get(kmerList.getPosition(0)).toString());
        Assert.assertEquals("[2]", map.get(kmerList.getPosition(1)).toString());
    }

    @Test
    public void TestEditDistance() {
        VKmer kmer1 = new VKmer("ACGT");
        VKmer kmer2 = new VKmer("AAAACGT");

        Assert.assertEquals(kmer1.editDistance(kmer2), 3);
        Assert.assertEquals(kmer1.editDistance(kmer2), kmer2.editDistance(kmer1));
        Assert.assertEquals(kmer1.fracDissimilar(true, kmer2), .75f);

        kmer1.setAsCopy("");
        Assert.assertEquals(kmer1.editDistance(kmer2), kmer2.getKmerLetterLength());
        Assert.assertEquals(kmer1.editDistance(kmer2), kmer2.editDistance(kmer1));

        kmer2.setAsCopy("");
        Assert.assertEquals(kmer1.editDistance(kmer2), kmer2.getKmerLetterLength());
        Assert.assertEquals(kmer1.editDistance(kmer2), kmer2.editDistance(kmer1));

    }

    @Test
    public void TestLargeKmerMergeFF() {
        VKmer kmer1 = new VKmer("GCGTACGCAGGATAGT");
        VKmer kmer2 = new VKmer("AGGATAGTATGTGAA");
        kmer1.mergeWithKmerInDir(EDGETYPE.FF, 9, kmer2);
        Assert.assertEquals("Invalid FF merge!!!", "GCGTACGCAGGATAGTATGTGAA", kmer1.toString());
    }

    @Test
    public void TestLargeKmerMergeFR() {
        VKmer kmer1 = new VKmer("GCGTACGCAGGATAGT");
        VKmer kmer2 = new VKmer("TTCACATACTATCCT");

        kmer1.mergeWithKmerInDir(EDGETYPE.FR, 9, kmer2);
        Assert.assertEquals("Invalid FR merge!!!", "GCGTACGCAGGATAGTATGTGAA", kmer1.toString());
    }

    @Test
    public void TestLargeKmerMergeRF() {
        VKmer kmer1 = new VKmer("ACTATCCTGCGTACGC");
        VKmer kmer2 = new VKmer("AGGATAGTATGTGAA");

        kmer1.mergeWithKmerInDir(EDGETYPE.RF, 9, kmer2);
        Assert.assertEquals("Invalid RF merge!!!", "TTCACATACTATCCTGCGTACGC", kmer1.toString());
    }

    @Test
    public void TestLargeKmerMergeRR() {
        VKmer kmer1 = new VKmer("ACTATCCTGCGTACGC");
        VKmer kmer2 = new VKmer("TTCACATACTATCCT");

        kmer1.mergeWithKmerInDir(EDGETYPE.RR, 9, kmer2);
        Assert.assertEquals("Invalid RR merge!!!", "TTCACATACTATCCTGCGTACGC", kmer1.toString());
    }

    private static final char[] symbols = new char[4];
    static {
        symbols[0] = 'A';
        symbols[1] = 'C';
        symbols[2] = 'G';
        symbols[3] = 'T';
    }

    public static String generateString(int length) {
        Random random = new Random();
        char[] buf = new char[length];
        for (int idx = 0; idx < buf.length; idx++) {
            buf[idx] = symbols[random.nextInt(4)];
        }
        return new String(buf);
    }

    @Test
    public void TestIndexOfForShortRead() {
        VKmer kmer1 = new VKmer("ACTATCCTGCGTACGC");
        VKmer kmer2 = new VKmer("TGCGT");
        Assert.assertEquals(7, kmer1.indexOf(kmer2));
        VKmer kmer3 = new VKmer("ACTATCCTGCGTACGC");
        VKmer kmer4 = new VKmer("TGCGA");
        Assert.assertEquals(-1, kmer3.indexOf(kmer4));
        VKmer kmer5 = new VKmer("ACTATCCTGCGTACGC");
        VKmer kmer6 = new VKmer("ACGC");
        Assert.assertEquals(12, kmer5.indexOf(kmer6));
        VKmer kmer7 = new VKmer("ACTATCCTGCGTACGC");
        VKmer kmer8 = new VKmer("ACTAC");
        Assert.assertEquals(-1, kmer7.indexOf(kmer8));
    }

    @Test
    public void TestIndexOfForLongRead() {
        String testStr1 = generateString(100);
        VKmer testKmer1 = new VKmer(testStr1);
        String subStr1 = testStr1.substring(25, 80);
        VKmer subKmer1 = new VKmer(subStr1);
        Assert.assertEquals(25, testKmer1.indexOf(subKmer1));

        String testStr2 = generateString(200);
        VKmer testKmer2 = new VKmer(testStr2);
        String subStr2 = testStr2.substring(100, 200);
        VKmer subKmer2 = new VKmer(subStr2);
        Assert.assertEquals(100, testKmer2.indexOf(subKmer2));

        String testStr3 = generateString(300);
        VKmer testKmer3 = new VKmer(testStr3);
        VKmer subKmer3 = new VKmer();
        for (int i = 0; i < 10; i++) {
            String subStr3 = testStr3.substring(40 + i * 3, 40 + i * 3 + 55);
            subKmer3.setAsCopy(subStr3);
            Assert.assertEquals(40 + i * 3, testKmer3.indexOf(subKmer3));
        }

        String testStr4 = generateString(55);
        if (!testStr3.contains(testStr4)) {
            VKmer testKmer4 = new VKmer(testStr4);
            Assert.assertEquals(-1, testKmer3.indexOf(testKmer4));
        }
    }
    
    @Test
    public void TestIndexOfRangeQuery() throws IOException{
        VKmer kmer1 = new VKmer("ACTATCCTGCGTACGC");
        VKmer kmer2 = new VKmer("GTGCGTC");
        Assert.assertEquals(7, kmer1.indexOfRangeQuery(kmer2, 1, 5, 3, 14));
        VKmer kmer3 = new VKmer("TGCGTACGC");
        VKmer kmer4 = new VKmer("GTGCGTC");
        Assert.assertEquals(0, kmer3.indexOfRangeQuery(kmer4, 1, 5, 0, 7));
        VKmer kmer5 = new VKmer("CCCGACTGCGT");
        VKmer kmer6 = new VKmer("GTGCGTC");
        Assert.assertEquals(6, kmer5.indexOfRangeQuery(kmer6, 1, 5, 0, 10));
        VKmer kmer7 = new VKmer("CCCGACTGCGT");
        VKmer kmer8 = new VKmer("GTGGGTC");
        Assert.assertEquals(-1, kmer7.indexOfRangeQuery(kmer8, 1, 5, 0, 10));
    }

}
