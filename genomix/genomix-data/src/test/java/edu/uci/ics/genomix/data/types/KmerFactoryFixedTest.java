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

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.genomix.data.types.KmerFactory;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.utils.GeneCode;

public class KmerFactoryFixedTest {
    static byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };

    KmerFactory kmerFactory = new KmerFactory(9);

    @Test
    public void TestGetKmerByRead(){
        VKmer vkmer = new VKmer();
        vkmer.setAsCopy(kmerFactory.getKmerByRead(9, array, 0));
        Assert.assertEquals("AGCTGACCG", vkmer.toString());
    }
    
    @Test
    public void TestGetKmerByReadReverse(){
        VKmer vkmer = new VKmer();
        vkmer.setAsCopy(kmerFactory.getKmerByReadReverse(9, array, 0));
        Assert.assertEquals("CGGTCAGCT", vkmer.toString());
    }
    
    @Test
    public void TestGetLastKmerFromChain() {
        VKmer vkmer = new VKmer();
        vkmer.setFromStringBytes(9, array, 0);
        Assert.assertEquals("AGCTGACCG", vkmer.toString());
        VKmer lastKmer;
        for (int i = 8; i > 0; i--) {
            lastKmer = kmerFactory.getLastKmerFromChain(i, vkmer);
            Assert.assertEquals("AGCTGACCG".substring(9 - i), lastKmer.toString());
            lastKmer = kmerFactory.getSubKmerFromChain(9 - i, i, vkmer);
            Assert.assertEquals("AGCTGACCG".substring(9 - i), lastKmer.toString());
        }
    }

    @Test
    public void TestGetFirstKmerFromChain() {
        VKmer vkmer = new VKmer();
        vkmer.setFromStringBytes(9, array, 0);
        Assert.assertEquals("AGCTGACCG", vkmer.toString());
        VKmer firstKmer;
        for (int i = 8; i > 0; i--) {
            firstKmer = kmerFactory.getFirstKmerFromChain(i, vkmer);
            Assert.assertEquals("AGCTGACCG".substring(0, i), firstKmer.toString());
            firstKmer = kmerFactory.getSubKmerFromChain(0, i, vkmer);
            Assert.assertEquals("AGCTGACCG".substring(0, i), firstKmer.toString());
        }
    }

    @Test
    public void TestGetSubKmer() {
        VKmer vkmer = new VKmer();
        vkmer.setFromStringBytes(9, array, 0);
        Assert.assertEquals("AGCTGACCG", vkmer.toString());
        VKmer subKmer;
        for (int istart = 0; istart < vkmer.getKmerLetterLength() - 1; istart++) {
            for (int isize = 1; isize + istart <= vkmer.getKmerLetterLength(); isize++) {
                subKmer = kmerFactory.getSubKmerFromChain(istart, isize, vkmer);
                Assert.assertEquals("AGCTGACCG".substring(istart, istart + isize), subKmer.toString());
            }
        }
    }

    @Test
    public void TestMergeNext() {
        VKmer vkmer = new VKmer();
        vkmer.setFromStringBytes(9, array, 0);
        Assert.assertEquals("AGCTGACCG", vkmer.toString());

        String text = "AGCTGACCG";
        for (byte x = GeneCode.A; x <= GeneCode.T; x++) {
            VKmer newkmer = kmerFactory.mergeKmerWithNextCode(vkmer, x);
            text = text + (char) GeneCode.GENE_SYMBOL[x];
            Assert.assertEquals(text, newkmer.toString());
            vkmer = new VKmer(newkmer);
        }
    }

    @Test
    public void TestMergePre() {
        VKmer vkmer = new VKmer();
        vkmer.setFromStringBytes(9, array, 0);
        Assert.assertEquals("AGCTGACCG", vkmer.toString());
        String text = "AGCTGACCG";
        for (byte x = GeneCode.A; x <= GeneCode.T; x++) {
            VKmer newkmer = kmerFactory.mergeKmerWithPreCode(vkmer, x);
            text = (char) GeneCode.GENE_SYMBOL[x] + text;
            Assert.assertEquals(text, newkmer.toString());
            vkmer = new VKmer(newkmer);
        }
    }

    @Test
    public void TestMergeTwoKmer() {
        VKmer vkmer1 = new VKmer();
        vkmer1.setFromStringBytes(9, array, 0);
        String text1 = "AGCTGACCG";
        VKmer vkmer2 = new VKmer();
        vkmer2.setFromStringBytes(9, array, 1);
        String text2 = "GCTGACCGT";
        Assert.assertEquals(text1, vkmer1.toString());
        Assert.assertEquals(text2, vkmer2.toString());

        VKmer merged = kmerFactory.mergeTwoKmer(vkmer1, vkmer2);
        Assert.assertEquals(text1 + text2, merged.toString());

        VKmer vkmer3 = new VKmer();
        vkmer3.setFromStringBytes(3, array, 1);
        String text3 = "GCT";
        Assert.assertEquals(text3, vkmer3.toString());

        merged = kmerFactory.mergeTwoKmer(vkmer1, vkmer3);
        Assert.assertEquals(text1 + text3, merged.toString());
        merged = kmerFactory.mergeTwoKmer(vkmer3, vkmer1);
        Assert.assertEquals(text3 + text1, merged.toString());

        VKmer vkmer4 = new VKmer();
        vkmer4.setFromStringBytes(8, array, 0);
        String text4 = "AGCTGACC";
        Assert.assertEquals(text4, vkmer4.toString());
        merged = kmerFactory.mergeTwoKmer(vkmer4, vkmer3);
        Assert.assertEquals(text4 + text3, merged.toString());

        VKmer vkmer5 = new VKmer();
        vkmer5.setFromStringBytes(7, array, 0);
        String text5 = "AGCTGAC";
        VKmer vkmer6 = new VKmer();
        vkmer6.setFromStringBytes(9, array, 1);
        String text6 = "GCTGACCGT";
        merged = kmerFactory.mergeTwoKmer(vkmer5, vkmer6);
        Assert.assertEquals(text5 + text6, merged.toString());

        vkmer6.setFromStringBytes(6, array, 1);
        String text7 = "GCTGAC";
        merged = kmerFactory.mergeTwoKmer(vkmer5, vkmer6);
        Assert.assertEquals(text5 + text7, merged.toString());

        vkmer6.setFromStringBytes(4, array, 1);
        String text8 = "GCTG";
        merged = kmerFactory.mergeTwoKmer(vkmer5, vkmer6);
        Assert.assertEquals(text5 + text8, merged.toString());
    }

    @Test
    public void TestShift() {
        VKmer vkmer = new VKmer(kmerFactory.getKmerByRead(9, array, 0));
        String text = "AGCTGACCG";
        Assert.assertEquals(text, vkmer.toString());

        VKmer kmerForward = kmerFactory.shiftKmerWithNextCode(vkmer, GeneCode.A);
        Assert.assertEquals(text, vkmer.toString());
        Assert.assertEquals("GCTGACCGA", kmerForward.toString());
        VKmer kmerBackward = kmerFactory.shiftKmerWithPreCode(vkmer, GeneCode.C);
        Assert.assertEquals(text, vkmer.toString());
        Assert.assertEquals("CAGCTGACC", kmerBackward.toString());

    }

    @Test
    public void TestReverseKmer() {
        VKmer vkmer = new VKmer();
        vkmer.setFromStringBytes(7, array, 0);
        Assert.assertEquals(vkmer.toString(), "AGCTGAC");
        VKmer reversed = kmerFactory.reverse(vkmer);
        Assert.assertEquals(reversed.toString(), "CAGTCGA");
        
        vkmer.setFromStringBytes(8, ("AATAGAAC").getBytes(), 0);
        Assert.assertEquals(vkmer.toString(), "AATAGAAC");
        reversed.reset(8);
        reversed = kmerFactory.reverse(vkmer);
        Assert.assertEquals(reversed.toString(), "CAAGATAA");
    }
}
