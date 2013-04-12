package edu.uci.ics.genomix.example.kmer;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.KmerUtil;

public class KmerUtilTest {
	static byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C','G','T'};
	
	@Test
	public void TestDegree(){
		Assert.assertTrue(KmerUtil.inDegree((byte) 0xff) == 4); 
		Assert.assertTrue(KmerUtil.outDegree((byte) 0xff) == 4);
		Assert.assertTrue(KmerUtil.inDegree((byte) 0x3f) == 2);
		Assert.assertTrue(KmerUtil.outDegree((byte) 0x01) == 1);
		Assert.assertTrue(KmerUtil.inDegree((byte) 0x01) == 0);
	}
	
	@Test
	public void TestGetLastKmer(){
		byte[] kmerChain = Kmer.compressKmer(9, array, 0);
		Assert.assertEquals("AGCTGACCG", Kmer.recoverKmerFrom(9, kmerChain, 0, kmerChain.length));
		byte[] lastKmer ;
		for(int i = 8; i>0 ; i--){
			lastKmer = KmerUtil.getLastKmerFromChain(i, 9, kmerChain, 0, kmerChain.length);
//			System.out.println(Kmer.recoverKmerFrom(i, lastKmer, 0, lastKmer.length));
			Assert.assertEquals("AGCTGACCG".substring(9-i), Kmer.recoverKmerFrom(i, lastKmer, 0, lastKmer.length));
		}
	}
	
	@Test
	public void TestMergeNext(){
		byte[] kmer = Kmer.compressKmer(9, array, 0);
		String text = "AGCTGACCG";
		Assert.assertEquals(text, Kmer.recoverKmerFrom(9, kmer, 0, kmer.length));
		for(byte x = Kmer.GENE_CODE.A; x<= Kmer.GENE_CODE.T ; x++){
			kmer = KmerUtil.mergeKmerWithNextCode(9+x, kmer, 0, kmer.length, x);
//			System.out.println(Kmer.recoverKmerFrom(9+x+1, kmer, 0, kmer.length));
			text = text + (char)Kmer.GENE_SYMBOL[x];
			Assert.assertEquals(text, Kmer.recoverKmerFrom(9+x+1, kmer, 0, kmer.length));
		}
		for(byte x = Kmer.GENE_CODE.A; x<= Kmer.GENE_CODE.T ; x++){
			kmer = KmerUtil.mergeKmerWithNextCode(13+x, kmer,0, kmer.length, x);
//			System.out.println(Kmer.recoverKmerFrom(13+x+1, kmer, 0, kmer.length));
			text = text + (char)Kmer.GENE_SYMBOL[x];
			Assert.assertEquals(text, Kmer.recoverKmerFrom(13+x+1, kmer, 0, kmer.length));
		}
	}
	
	@Test
	public void TestMergePre(){
		byte[] kmer = Kmer.compressKmer(9, array, 0);
		String text = "AGCTGACCG";
		Assert.assertEquals(text, Kmer.recoverKmerFrom(9, kmer, 0, kmer.length));
		for(byte x = Kmer.GENE_CODE.A; x<= Kmer.GENE_CODE.T ; x++){
			kmer = KmerUtil.mergeKmerWithPreCode(9+x, kmer, 0, kmer.length,x);
//			System.out.println(Kmer.recoverKmerFrom(9+x+1, kmer, 0, kmer.length));
			text = (char)Kmer.GENE_SYMBOL[x] + text;
			Assert.assertEquals(text , Kmer.recoverKmerFrom(9+x+1, kmer, 0, kmer.length));
		}
		for(byte x = Kmer.GENE_CODE.A; x<= Kmer.GENE_CODE.T ; x++){
			kmer = KmerUtil.mergeKmerWithPreCode(13+x, kmer,0, kmer.length, x);
//			System.out.println(Kmer.recoverKmerFrom(13+x+1, kmer, 0, kmer.length));
			text = (char)Kmer.GENE_SYMBOL[x] + text;
			Assert.assertEquals(text , Kmer.recoverKmerFrom(13+x+1, kmer, 0, kmer.length));
		}
	}
	
	@Test
	public void TestMergeTwoKmer(){
		byte[] kmer1 = Kmer.compressKmer(9, array, 0);
		String text1 = "AGCTGACCG";
		byte[] kmer2 = Kmer.compressKmer(9, array, 1);
		String text2 = "GCTGACCGT";
		Assert.assertEquals(text1, Kmer.recoverKmerFrom(9, kmer1, 0, kmer1.length));
		Assert.assertEquals(text2, Kmer.recoverKmerFrom(9, kmer2, 0, kmer2.length));
		
		byte[] merged = KmerUtil.mergeTwoKmer(9, kmer1,0,kmer1.length, 9, kmer2,0,kmer2.length);
		Assert.assertEquals(text1+text2, Kmer.recoverKmerFrom(9+9, merged, 0, merged.length));
		
		byte[] kmer3 = Kmer.compressKmer(3, array, 1);
		String text3 = "GCT";
		Assert.assertEquals(text3, Kmer.recoverKmerFrom(3, kmer3, 0, kmer3.length));
		merged = KmerUtil.mergeTwoKmer(9, kmer1, 0 , kmer1.length, 3, kmer3, 0, kmer3.length);
		Assert.assertEquals(text1+text3, Kmer.recoverKmerFrom(9+3, merged, 0, merged.length));
		merged = KmerUtil.mergeTwoKmer(3, kmer3, 0 , kmer3.length, 9, kmer1, 0, kmer1.length);
		Assert.assertEquals(text3+text1, Kmer.recoverKmerFrom(9+3, merged, 0, merged.length));
		
		byte[] kmer4 = Kmer.compressKmer(8, array, 0);
		String text4 = "AGCTGACC";
		Assert.assertEquals(text4, Kmer.recoverKmerFrom(8, kmer4, 0, kmer4.length));
		merged = KmerUtil.mergeTwoKmer(8, kmer4, 0, kmer4.length, 3, kmer3, 0, kmer3.length);
		Assert.assertEquals(text4+text3, Kmer.recoverKmerFrom(8+3, merged, 0, merged.length));
		
		byte[] kmer5 = Kmer.compressKmer(7, array, 0);
		String text5 = "AGCTGAC";
		byte[] kmer6 = Kmer.compressKmer(9, array, 1);
		String text6 = "GCTGACCGT";
		merged = KmerUtil.mergeTwoKmer(7, kmer5, 0, kmer5.length,9, kmer6, 0, kmer6.length);
		Assert.assertEquals(text5+text6, Kmer.recoverKmerFrom(7+9, merged, 0, merged.length));
		
		byte[] kmer7 = Kmer.compressKmer(6, array, 1);
		String text7 = "GCTGAC";
		merged = KmerUtil.mergeTwoKmer(7, kmer5, 0, kmer5.length, 6, kmer7, 0, kmer7.length);
		Assert.assertEquals(text5+text7, Kmer.recoverKmerFrom(7+6, merged, 0, merged.length));

	}
	@Test 
	public void TestShift(){
		byte[] kmer = Kmer.compressKmer(9, array, 0);
		String text = "AGCTGACCG";
		Assert.assertEquals(text, Kmer.recoverKmerFrom(9, kmer, 0, kmer.length));
		
		byte [] kmerForward = KmerUtil.shiftKmerWithNextCode(9, kmer,0, kmer.length, Kmer.GENE_CODE.A);
		Assert.assertEquals(text, Kmer.recoverKmerFrom(9, kmer, 0, kmer.length));
		Assert.assertEquals("GCTGACCGA", Kmer.recoverKmerFrom(9, kmerForward, 0, kmerForward.length));
		byte [] kmerBackward = KmerUtil.shiftKmerWithPreCode(9, kmer,0, kmer.length,Kmer.GENE_CODE.C);
		Assert.assertEquals(text, Kmer.recoverKmerFrom(9, kmer, 0, kmer.length));
		Assert.assertEquals("CAGCTGACC", Kmer.recoverKmerFrom(9, kmerBackward, 0, kmerBackward.length));
		
	}
	@Test
	public void TestGetGene(){
		byte[] kmer = Kmer.compressKmer(9, array, 0);
		String text = "AGCTGACCG";
		for(int i =0; i < 9; i++){
			Assert.assertEquals(text.charAt(i), 
					(char)(Kmer.GENE_CODE.getSymbolFromCode(KmerUtil.getGeneCodeAtPosition(i, 9, kmer, 0, kmer.length))));
		}
	}
}
