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
        KmerBytesWritable.setGlobalKmerLength(k);
        KmerBytesWritable kmer = new KmerBytesWritable();
        kmer.setByRead(array, 0);
        Assert.assertEquals(kmer.toString(), "AATAGAA");

        kmer.setByRead(array, 1);
        Assert.assertEquals(kmer.toString(), "ATAGAAG");
    }

    @Test
    public void TestMoveKmer() {
        KmerBytesWritable.setGlobalKmerLength(k);
        KmerBytesWritable kmer = new KmerBytesWritable();
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
        KmerBytesWritable.setGlobalKmerLength(k);
        KmerBytesWritable kmer = new KmerBytesWritable();
        kmer.setByRead(array, 0);
        Assert.assertEquals(kmer.toString(), "AATAGAA");

        kmer.setByReadReverse(array, 1);
        Assert.assertEquals(kmer.toString(), "CTTCTAT");
    }

    @Test
    public void TestMoveKmerReverse() {
        KmerBytesWritable.setGlobalKmerLength(k);
        KmerBytesWritable kmer = new KmerBytesWritable();
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
        KmerBytesWritable.setGlobalKmerLength(9);
        KmerBytesWritable kmer = new KmerBytesWritable();
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
            KmerBytesWritable.setGlobalKmerLength(k);
            KmerBytesWritable kmer = new KmerBytesWritable();
            KmerBytesWritable kmerAppend = new KmerBytesWritable();
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
}
