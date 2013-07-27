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

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;

public class KmerBytesWritableTest {
    static byte[] array = { 'A', 'A', 'T', 'A', 'G', 'A', 'A', 'G' };
    static int k = 7;

    @Test
    public void TestCompressKmer() {
        KmerBytesWritable kmer = new KmerBytesWritable(k);
        kmer.setByRead(array, 0);
        Assert.assertEquals(kmer.toString(), "AATAGAA");

        kmer.setByRead(array, 1);
        Assert.assertEquals(kmer.toString(), "ATAGAAG");
    }

    @Test
    public void TestMoveKmer() {
        KmerBytesWritable kmer = new KmerBytesWritable(k);
        kmer.setByRead(array, 0);
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
        KmerBytesWritable kmer = new KmerBytesWritable(k);
        kmer.setByRead(array, 0);
        Assert.assertEquals(kmer.toString(), "AATAGAA");

        kmer.setByReadReverse(array, 1);
        Assert.assertEquals(kmer.toString(), "CTTCTAT");
    }

    @Test
    public void TestMoveKmerReverse() {
        KmerBytesWritable kmer = new KmerBytesWritable(k);
        kmer.setByRead(array, 0);
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
        KmerBytesWritable kmer = new KmerBytesWritable(9);
        String text = "AGCTGACCG";
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G' };
        kmer.setByRead(array, 0);

        for (int i = 0; i < 9; i++) {
            Assert.assertEquals(text.charAt(i), (char) (GeneCode.getSymbolFromCode(kmer.getGeneCodeAtPosition(i))));
        }
    }

    @Test
    public void TestGetOneByteFromKmer() {
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };
        String string = "AGCTGACCGT";
        for (int k = 3; k <= 10; k++) {
            KmerBytesWritable kmer = new KmerBytesWritable(k);
            KmerBytesWritable kmerAppend = new KmerBytesWritable(k);
            kmer.setByRead(array, 0);
            Assert.assertEquals(string.substring(0, k), kmer.toString());
            for (int b = 0; b < k; b++) {
                byte byteActual = KmerBytesWritable.getOneByteFromKmerAtPosition(b, kmer.getBytes(), kmer.getOffset(),
                        kmer.getLength());
                byte byteExpect = GeneCode.getCodeFromSymbol(array[b]);
                for (int i = 1; i < 4 && b + i < k; i++) {
                    byteExpect += GeneCode.getCodeFromSymbol(array[b + i]) << (i * 2);
                }
                Assert.assertEquals(byteActual, byteExpect);
                KmerBytesWritable.appendOneByteAtPosition(b, byteActual, kmerAppend.getBytes(), kmerAppend.getOffset(),
                        kmerAppend.getLength());
            }
            Assert.assertEquals(kmer.toString(), kmerAppend.toString());
        }
    }

    @Test
    public void TestMergeFFKmer() {
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };
        String text = "AGCTGACCGT";
        KmerBytesWritable kmer1 = new KmerBytesWritable(8);
        kmer1.setByRead(array, 0);
        String text1 = "AGCTGACC";
        KmerBytesWritable kmer2 = new KmerBytesWritable(8);
        kmer2.setByRead(array, 1);
        String text2 = "GCTGACCG";
        Assert.assertEquals(text2, kmer2.toString());
        KmerBytesWritable merge = new KmerBytesWritable(kmer1);
        int kmerSize = 8;
        merge.mergeWithFFKmer(kmerSize, kmer2);
        Assert.assertEquals(text1 + text2.substring(kmerSize - 1), merge.toString());

        for (int i = 1; i < 8; i++) {
            merge.set(kmer1);
            merge.mergeWithFFKmer(i, kmer2);
            Assert.assertEquals(text1 + text2.substring(i - 1), merge.toString());
        }

        for (int ik = 1; ik <= 10; ik++) {
            for (int jk = 1; jk <= 10; jk++) {
                kmer1 = new KmerBytesWritable(ik);
                kmer2 = new KmerBytesWritable(jk);
                kmer1.setByRead(array, 0);
                kmer2.setByRead(array, 0);
                text1 = text.substring(0, ik);
                text2 = text.substring(0, jk);
                Assert.assertEquals(text1, kmer1.toString());
                Assert.assertEquals(text2, kmer2.toString());
                for (int x = 1; x < jk; x++) {
                    merge.set(kmer1);
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
        KmerBytesWritable kmer1 = new KmerBytesWritable(text1.length());
        kmer1.setByRead(resultArray, 0);
        Assert.assertEquals(text1, kmer1.toString());
        
        // kmer2 is the rc of the end of the read
        String text2 = "GGTTGTT";
        KmerBytesWritable kmer2 = new KmerBytesWritable(text2.length());
        kmer2.setByReadReverse(resultArray, result.length() - text2.length());
        Assert.assertEquals(text2, kmer2.toString());
        
        KmerBytesWritable merge = new KmerBytesWritable(kmer1);
        merge.mergeWithFRKmer(kmerSize, kmer2);
        Assert.assertEquals(result, merge.toString());
        
        int i = 1;
        merge.set(kmer1);
        merge.mergeWithFRKmer(i, kmer2);
        Assert.assertEquals("AAGCTAAAACAACC", merge.toString());
        
        i = 2;
        merge.set(kmer1);
        merge.mergeWithFRKmer(i, kmer2);
        Assert.assertEquals("AAGCTAAACAACC", merge.toString());
        
        i = 3;
        merge.set(kmer1);
        merge.mergeWithFRKmer(i, kmer2);
        Assert.assertEquals("AAGCTAACAACC", merge.toString());
    }
    
    
    @Test
    public void TestMergeRFKmer() {
        int kmerSize = 3;
        String result = "GGCACAACAACCC";
        byte[] resultArray = result.getBytes();
        
        String text1 = "AACAACCC";
        KmerBytesWritable kmer1 = new KmerBytesWritable(text1.length());
        kmer1.setByRead(resultArray, 5);
        Assert.assertEquals(text1, kmer1.toString());
        
        // kmer2 is the rc of the end of the read
        String text2 = "TTGTGCC";
        KmerBytesWritable kmer2 = new KmerBytesWritable(text2.length());
        kmer2.setByReadReverse(resultArray, 0);
        Assert.assertEquals(text2, kmer2.toString());
        
        KmerBytesWritable merge = new KmerBytesWritable(kmer1);
        merge.mergeWithRFKmer(kmerSize, kmer2);
        Assert.assertEquals(result, merge.toString());
        
        int i = 1;
        merge.set(kmer1);
        merge.mergeWithRFKmer(i, kmer2);
        Assert.assertEquals("GGCACAAAACAACCC", merge.toString());
        
        i = 2;
        merge.set(kmer1);
        merge.mergeWithRFKmer(i, kmer2);
        Assert.assertEquals("GGCACAAACAACCC", merge.toString());
        
        i = 3;
        merge.set(kmer1);
        merge.mergeWithRFKmer(i, kmer2);
        Assert.assertEquals("GGCACAACAACCC", merge.toString());
        
        String test1;
        String test2;
        test1 = "CTA";
        test2 = "AGA";
        KmerBytesWritable k1 = new KmerBytesWritable(3);
        KmerBytesWritable k2 = new KmerBytesWritable(3);
        k1.setByRead(test1.getBytes(), 0);
        k2.setByRead(test2.getBytes(), 0);
        k1.mergeWithRFKmer(3, k2);
        Assert.assertEquals("TCTA", k1.toString());
        
        test1 = "CTA";
        test2 = "ATA"; //TAT
        k1 = new KmerBytesWritable(3);
        k2 = new KmerBytesWritable(3);
        k1.setByRead(test1.getBytes(), 0);
        k2.setByRead(test2.getBytes(), 0);
        k1.mergeWithFRKmer(3, k2);
        Assert.assertEquals("CTAT", k1.toString());
        
        test1 = "ATA";
        test2 = "CTA"; //TAT
        k1 = new KmerBytesWritable(3);
        k2 = new KmerBytesWritable(3);
        k1.setByRead(test1.getBytes(), 0);
        k2.setByRead(test2.getBytes(), 0);
        k1.mergeWithFRKmer(3, k2);
        Assert.assertEquals("ATAG", k1.toString());
        
        test1 = "TCTAT";
        test2 = "GAAC";
        k1 = new KmerBytesWritable(5);
        k2 = new KmerBytesWritable(4);
        k1.setByRead(test1.getBytes(), 0);
        k2.setByRead(test2.getBytes(), 0);
        k1.mergeWithRFKmer(3, k2);
        Assert.assertEquals("GTTCTAT", k1.toString());
    }
    
    

    @Test
    public void TestMergeRRKmer() {
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };
        String text = "AGCTGACCGT";
        KmerBytesWritable kmer1 = new KmerBytesWritable(8);
        kmer1.setByRead(array, 0);
        String text1 = "AGCTGACC";
        KmerBytesWritable kmer2 = new KmerBytesWritable(8);
        kmer2.setByRead(array, 1);
        String text2 = "GCTGACCG";
        Assert.assertEquals(text2, kmer2.toString());
        KmerBytesWritable merge = new KmerBytesWritable(kmer2);
        int kmerSize = 8;
        merge.mergeWithRRKmer(kmerSize, kmer1);
        Assert.assertEquals(text1 + text2.substring(kmerSize - 1), merge.toString());

        for (int i = 1; i < 8; i++) {
            merge.set(kmer2);
            merge.mergeWithRRKmer(i, kmer1);
            Assert.assertEquals(text1.substring(0, text1.length() - i + 1) + text2, merge.toString());
        }

        for (int ik = 1; ik <= 10; ik++) {
            for (int jk = 1; jk <= 10; jk++) {
                kmer1 = new KmerBytesWritable(ik);
                kmer2 = new KmerBytesWritable(jk);
                kmer1.setByRead(array, 0);
                kmer2.setByRead(array, 0);
                text1 = text.substring(0, ik);
                text2 = text.substring(0, jk);
                Assert.assertEquals(text1, kmer1.toString());
                Assert.assertEquals(text2, kmer2.toString());
                for (int x = 1; x < ik; x++) {
                    merge.set(kmer2);
                    merge.mergeWithRRKmer(x, kmer1);
                    Assert.assertEquals(text1.substring(0, text1.length() - x + 1) + text2, merge.toString());
                }
            }
        }
    }
    
    @Test
    public void TestFinalMerge() {
        String selfString;
        String match;
        String msgString;
        int index;
        KmerBytesWritable kmer = new KmerBytesWritable();
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
        kmer.reset(msgString.length() - index);
        kmer.setByRead(msgString.substring(index).getBytes(), 0);
        System.out.println(kmer.toString());
        
        //FR test
        selfString = F1;
        match = selfString.substring(selfString.length() - kmerSize + 1,selfString.length()); 
        msgString = GeneCode.reverseComplement(R2);
        index = msgString.indexOf(match);
        kmer.reset(msgString.length() - index);
        kmer.setByRead(msgString.substring(index).getBytes(), 0);
        System.out.println(kmer.toString());
        
        //RF test
        selfString = R1;
        match = selfString.substring(0,kmerSize - 1); 
        msgString = GeneCode.reverseComplement(F2);
        index = msgString.lastIndexOf(match) + kmerSize - 2;
        kmer.reset(index + 1);
        kmer.setByReadReverse(msgString.substring(0, index + 1).getBytes(), 0);
        System.out.println(kmer.toString());
        
        //RR test
        selfString = R1;
        match = selfString.substring(0,kmerSize - 1); 
        msgString = R2;
        index = msgString.lastIndexOf(match) + kmerSize - 2;
        kmer.reset(index + 1);
        kmer.setByRead(msgString.substring(0, index + 1).getBytes(), 0);
        System.out.println(kmer.toString());
    }
}
