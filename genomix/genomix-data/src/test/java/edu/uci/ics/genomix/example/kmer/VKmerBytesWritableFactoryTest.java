package edu.uci.ics.genomix.example.kmer;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritableFactory;

public class VKmerBytesWritableFactoryTest {
    static byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C', 'G', 'T' };

    VKmerBytesWritableFactory kmerFactory = new VKmerBytesWritableFactory(8);

    @Test
    public void TestDegree() {
        Assert.assertTrue(GeneCode.inDegree((byte) 0xff) == 4);
        Assert.assertTrue(GeneCode.outDegree((byte) 0xff) == 4);
        Assert.assertTrue(GeneCode.inDegree((byte) 0x3f) == 2);
        Assert.assertTrue(GeneCode.outDegree((byte) 0x01) == 1);
        Assert.assertTrue(GeneCode.inDegree((byte) 0x01) == 0);
    }

    @Test
    public void TestGetLastKmer() {
        KmerBytesWritable kmer = new KmerBytesWritable(9);
        kmer.setByRead(array, 0);
        Assert.assertEquals("AGCTGACCG", kmer.toString());
        KmerBytesWritable lastKmer;
        for (int i = 8; i > 0; i--) {
            lastKmer = kmerFactory.getLastKmerFromChain(i, kmer);
            Assert.assertEquals("AGCTGACCG".substring(9 - i), lastKmer.toString());
        }
        VKmerBytesWritable vlastKmer;
        for (int i = 8; i > 0; i--) {
            vlastKmer = kmerFactory.getLastKmerFromChain(i, kmer);
            Assert.assertEquals("AGCTGACCG".substring(9 - i), vlastKmer.toString());
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
        VKmerBytesWritable kmer6 = new VKmerBytesWritable(9);
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
        VKmerBytesWritable kmer = new VKmerBytesWritable(kmerFactory.getKmerByRead(9, array, 0));
        String text = "AGCTGACCG";
        Assert.assertEquals(text, kmer.toString());

        VKmerBytesWritable kmerForward = kmerFactory.shiftKmerWithNextCode(kmer, GeneCode.A);
        Assert.assertEquals(text, kmer.toString());
        Assert.assertEquals("GCTGACCGA", kmerForward.toString());
        VKmerBytesWritable kmerBackward = kmerFactory.shiftKmerWithPreCode(kmer, GeneCode.C);
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
