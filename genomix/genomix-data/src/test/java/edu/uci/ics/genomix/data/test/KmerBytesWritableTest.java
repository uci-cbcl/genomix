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
    public void TestMergeNext() {
        KmerBytesWritable kmer = new KmerBytesWritable(9);
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G' };
        kmer.setByRead(array, 0);
        Assert.assertEquals("AGCTGACCG", kmer.toString());

        String text = "AGCTGACCG";
        for (int i = 0; i < 10; i++) {
            for (byte x = GeneCode.A; x <= GeneCode.T; x++) {
                kmer.mergeNextCode(x);
                text = text + (char) GeneCode.GENE_SYMBOL[x];
                Assert.assertEquals(text, kmer.toString());
            }
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
    public void TestMergeNextKmer() {
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
        merge.mergeNextKmer(kmerSize, kmer2);
        Assert.assertEquals(text1 + text2.substring(kmerSize - 1), merge.toString());

        for (int i = 1; i < 8; i++) {
            merge.set(kmer1);
            merge.mergeNextKmer(i, kmer2);
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
                    merge.mergeNextKmer(x, kmer2);
                    Assert.assertEquals(text1 + text2.substring(x - 1), merge.toString());
                }
            }
        }
    }

    @Test
    public void TestMergePreKmer() {
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
        merge.mergePreKmer(kmerSize, kmer1);
        Assert.assertEquals(text1 + text2.substring(kmerSize - 1), merge.toString());

        for (int i = 1; i < 8; i++) {
            merge.set(kmer2);
            merge.mergePreKmer(i, kmer1);
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
                    merge.mergePreKmer(x, kmer1);
                    Assert.assertEquals(text1.substring(0, text1.length() - x + 1) + text2, merge.toString());
                }
            }
        }
    }

}
