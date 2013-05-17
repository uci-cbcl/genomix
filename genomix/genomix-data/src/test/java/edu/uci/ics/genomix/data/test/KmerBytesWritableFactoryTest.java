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

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerBytesWritableFactory;

public class KmerBytesWritableFactoryTest {
    static byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };

    KmerBytesWritableFactory kmerFactory = new KmerBytesWritableFactory(8);

    @Test
    public void TestGetLastKmer() {
        KmerBytesWritable kmer = new KmerBytesWritable(9);
        kmer.setByRead(array, 0);
        Assert.assertEquals("AGCTGACCG", kmer.toString());
        KmerBytesWritable lastKmer;
        for (int i = 8; i > 0; i--) {
            lastKmer = kmerFactory.getLastKmerFromChain(i, kmer);
            Assert.assertEquals("AGCTGACCG".substring(9 - i), lastKmer.toString());
            lastKmer = kmerFactory.getSubKmerFromChain(9 - i, i, kmer);
            Assert.assertEquals("AGCTGACCG".substring(9 - i), lastKmer.toString());
        }
        KmerBytesWritable vlastKmer;
        for (int i = 8; i > 0; i--) {
            vlastKmer = kmerFactory.getLastKmerFromChain(i, kmer);
            Assert.assertEquals("AGCTGACCG".substring(9 - i), vlastKmer.toString());
            vlastKmer = kmerFactory.getSubKmerFromChain(9 - i, i, kmer);
            Assert.assertEquals("AGCTGACCG".substring(9 - i), vlastKmer.toString());
        }
    }

    @Test
    public void TestGetFirstKmer() {
        KmerBytesWritable kmer = new KmerBytesWritable(9);
        kmer.setByRead(array, 0);
        Assert.assertEquals("AGCTGACCG", kmer.toString());
        KmerBytesWritable firstKmer;
        for (int i = 8; i > 0; i--) {
            firstKmer = kmerFactory.getFirstKmerFromChain(i, kmer);
            Assert.assertEquals("AGCTGACCG".substring(0, i), firstKmer.toString());
            firstKmer = kmerFactory.getSubKmerFromChain(0, i, kmer);
            Assert.assertEquals("AGCTGACCG".substring(0, i), firstKmer.toString());
        }
        KmerBytesWritable vfirstKmer;
        for (int i = 8; i > 0; i--) {
            vfirstKmer = kmerFactory.getFirstKmerFromChain(i, kmer);
            Assert.assertEquals("AGCTGACCG".substring(0, i), vfirstKmer.toString());
            vfirstKmer = kmerFactory.getSubKmerFromChain(0, i, kmer);
            Assert.assertEquals("AGCTGACCG".substring(0, i), vfirstKmer.toString());
        }
    }

    @Test
    public void TestGetSubKmer() {
        KmerBytesWritable kmer = new KmerBytesWritable(9);
        kmer.setByRead(array, 0);
        Assert.assertEquals("AGCTGACCG", kmer.toString());
        KmerBytesWritable subKmer;
        for (int istart = 0; istart < kmer.getKmerLength() - 1; istart++) {
            for (int isize = 1; isize + istart <= kmer.getKmerLength(); isize++) {
                subKmer = kmerFactory.getSubKmerFromChain(istart, isize, kmer);
                Assert.assertEquals("AGCTGACCG".substring(istart, istart + isize), subKmer.toString());
            }
        }
    }

    @Test
    public void TestMergeNext() {
        KmerBytesWritable kmer = new KmerBytesWritable(9);
        kmer.setByRead(array, 0);
        Assert.assertEquals("AGCTGACCG", kmer.toString());

        String text = "AGCTGACCG";
        for (byte x = GeneCode.A; x <= GeneCode.T; x++) {
            KmerBytesWritable newkmer = kmerFactory.mergeKmerWithNextCode(kmer, x);
            text = text + (char) GeneCode.GENE_SYMBOL[x];
            Assert.assertEquals(text, newkmer.toString());
            kmer = new KmerBytesWritable(newkmer);
        }
        for (byte x = GeneCode.A; x <= GeneCode.T; x++) {
            KmerBytesWritable newkmer = kmerFactory.mergeKmerWithNextCode(kmer, x);
            text = text + (char) GeneCode.GENE_SYMBOL[x];
            Assert.assertEquals(text, newkmer.toString());
            kmer = new KmerBytesWritable(newkmer);
        }
    }

    @Test
    public void TestMergePre() {
        KmerBytesWritable kmer = new KmerBytesWritable(9);
        kmer.setByRead(array, 0);
        Assert.assertEquals("AGCTGACCG", kmer.toString());
        String text = "AGCTGACCG";
        for (byte x = GeneCode.A; x <= GeneCode.T; x++) {
            KmerBytesWritable newkmer = kmerFactory.mergeKmerWithPreCode(kmer, x);
            text = (char) GeneCode.GENE_SYMBOL[x] + text;
            Assert.assertEquals(text, newkmer.toString());
            kmer = new KmerBytesWritable(newkmer);
        }
        for (byte x = GeneCode.A; x <= GeneCode.T; x++) {
            KmerBytesWritable newkmer = kmerFactory.mergeKmerWithPreCode(kmer, x);
            text = (char) GeneCode.GENE_SYMBOL[x] + text;
            Assert.assertEquals(text, newkmer.toString());
            kmer = new KmerBytesWritable(newkmer);
        }
    }

    @Test
    public void TestMergeTwoKmer() {
        KmerBytesWritable kmer1 = new KmerBytesWritable(9);
        kmer1.setByRead(array, 0);
        String text1 = "AGCTGACCG";
        KmerBytesWritable kmer2 = new KmerBytesWritable(9);
        kmer2.setByRead(array, 1);
        String text2 = "GCTGACCGT";
        Assert.assertEquals(text1, kmer1.toString());
        Assert.assertEquals(text2, kmer2.toString());

        KmerBytesWritable merged = kmerFactory.mergeTwoKmer(kmer1, kmer2);
        Assert.assertEquals(text1 + text2, merged.toString());

        KmerBytesWritable kmer3 = new KmerBytesWritable(3);
        kmer3.setByRead(array, 1);
        String text3 = "GCT";
        Assert.assertEquals(text3, kmer3.toString());

        merged = kmerFactory.mergeTwoKmer(kmer1, kmer3);
        Assert.assertEquals(text1 + text3, merged.toString());
        merged = kmerFactory.mergeTwoKmer(kmer3, kmer1);
        Assert.assertEquals(text3 + text1, merged.toString());

        KmerBytesWritable kmer4 = new KmerBytesWritable(8);
        kmer4.setByRead(array, 0);
        String text4 = "AGCTGACC";
        Assert.assertEquals(text4, kmer4.toString());
        merged = kmerFactory.mergeTwoKmer(kmer4, kmer3);
        Assert.assertEquals(text4 + text3, merged.toString());

        KmerBytesWritable kmer5 = new KmerBytesWritable(7);
        kmer5.setByRead(array, 0);
        String text5 = "AGCTGAC";
        KmerBytesWritable kmer6 = new KmerBytesWritable(9);
        kmer6.setByRead(9, array, 1);
        String text6 = "GCTGACCGT";
        merged = kmerFactory.mergeTwoKmer(kmer5, kmer6);
        Assert.assertEquals(text5 + text6, merged.toString());

        kmer6.setByRead(6, array, 1);
        String text7 = "GCTGAC";
        merged = kmerFactory.mergeTwoKmer(kmer5, kmer6);
        Assert.assertEquals(text5 + text7, merged.toString());

        kmer6.setByRead(4, array, 1);
        String text8 = "GCTG";
        merged = kmerFactory.mergeTwoKmer(kmer5, kmer6);
        Assert.assertEquals(text5 + text8, merged.toString());

    }

    @Test
    public void TestShift() {
        KmerBytesWritable kmer = new KmerBytesWritable(kmerFactory.getKmerByRead(9, array, 0));
        String text = "AGCTGACCG";
        Assert.assertEquals(text, kmer.toString());

        KmerBytesWritable kmerForward = kmerFactory.shiftKmerWithNextCode(kmer, GeneCode.A);
        Assert.assertEquals(text, kmer.toString());
        Assert.assertEquals("GCTGACCGA", kmerForward.toString());
        KmerBytesWritable kmerBackward = kmerFactory.shiftKmerWithPreCode(kmer, GeneCode.C);
        Assert.assertEquals(text, kmer.toString());
        Assert.assertEquals("CAGCTGACC", kmerBackward.toString());

    }

    @Test
    public void TestReverseKmer() {
        KmerBytesWritable kmer = new KmerBytesWritable(7);
        kmer.setByRead(array, 0);
        Assert.assertEquals(kmer.toString(), "AGCTGAC");
        KmerBytesWritable reversed = kmerFactory.reverse(kmer);
        Assert.assertEquals(reversed.toString(), "CAGTCGA");
    }
}
