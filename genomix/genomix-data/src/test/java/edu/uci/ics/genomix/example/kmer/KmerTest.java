package edu.uci.ics.genomix.example.kmer;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.Kmer;

public class KmerTest {
	static byte[] array = { 'A', 'A', 'T', 'A', 'G', 'A', 'A', 'G' };
	static int k = 7;
	
	@Test
	public void TestCompressKmer() {
		byte[] kmer = Kmer.compressKmer(k, array, 0);
		String result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "AATAGAA");
		
		kmer = Kmer.compressKmer(k, array, 1);
		result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "ATAGAAG");
	}
	
	@Test
	public void TestMoveKmer(){
		byte[] kmer = Kmer.compressKmer(k, array, 0);
		String result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "AATAGAA");
		
		for (int i = k; i < array.length-1; i++) {
			Kmer.moveKmer(k, kmer, array[i]);
			Assert.assertTrue(false);
		}

		byte out = Kmer.moveKmer(k, kmer, array[array.length - 1]);
		Assert.assertEquals(out, Kmer.GENE_CODE.getAdjBit((byte) 'A'));
		result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "ATAGAAG");
		
	}
	
	@Test
	public void TestReverseKmer(){
		byte[] kmer = Kmer.compressKmer(k, array, 0);
		String result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "AATAGAA");
		byte[] reversed = Kmer.reverseKmer(k, kmer);
		result = Kmer.recoverKmerFrom(k, reversed, 0, kmer.length);
		Assert.assertEquals(result, "AAGATAA");
	}
	
	@Test
	public void TestCompressKmerReverse() {
		byte[] kmer = Kmer.compressKmerReverse(k, array, 0);
		String result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "AAGATAA");
		
		kmer = Kmer.compressKmerReverse(k, array, 1);
		result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "GAAGATA");
	}
	
	@Test
	public void TestMoveKmerReverse(){
		byte[] kmer = Kmer.compressKmerReverse(k, array, 0);
		String result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "AAGATAA");
		
		for (int i = k; i < array.length-1; i++) {
			Kmer.moveKmerReverse(k, kmer, array[i]);
			Assert.assertTrue(false);
		}

		byte out = Kmer.moveKmerReverse(k, kmer, array[array.length - 1]);
		Assert.assertEquals(out, Kmer.GENE_CODE.getAdjBit((byte) 'A'));
		result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "GAAGATA");
	}


}
