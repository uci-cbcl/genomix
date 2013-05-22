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
    public void TestMergeNextKmer(){
        byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };
        KmerBytesWritable kmer1 = new KmerBytesWritable(9);
        kmer1.setByRead(array, 0);
        String text1 = "AGCTGACCG";
        KmerBytesWritable kmer2 = new KmerBytesWritable(9);
        kmer2.setByRead(array, 1);
        String text2 = "GCTGACCGT";
        Assert.assertEquals(text1, kmer1.toString());
        Assert.assertEquals(text2, kmer2.toString());

        KmerBytesWritable merged = new KmerBytesWritable(kmer1);
        merged.mergeNextKmer(9, kmer2);
        Assert.assertEquals("AGCTGACCGT", merged);

        KmerBytesWritable kmer3 = new KmerBytesWritable(3);
        kmer3.setByRead(array, 1);
        String text3 = "GCT";
        Assert.assertEquals(text3, kmer3.toString());
        merged.mergeNextKmer(1, kmer3);
        Assert.assertEquals(text1 + text3, merged.toString());
    }

}
