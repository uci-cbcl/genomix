package edu.uci.ics.genomix.example.kmer;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.Kmer;

public class KmerTest {
	static byte[] array = { 'A', 'A', 'T', 'A', 'G', 'A', 'A', 'G' };
	static int k = 7;
	
	@Test
	public void TestCompressKmer() {
		byte[] kmer = Kmer.CompressKmer(k, array, 0);
		String result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "AATAGAA");
		
		kmer = Kmer.CompressKmer(k, array, 1);
		result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "ATAGAAG");
	}
	
	@Test
	public void TestMoveKmer(){
		byte[] kmer = Kmer.CompressKmer(k, array, 0);
		String result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "AATAGAA");
		
		for (int i = k; i < array.length-1; i++) {
			Kmer.MoveKmer(k, kmer, array[i]);
			Assert.assertTrue(false);
		}

		byte out = Kmer.MoveKmer(k, kmer, array[array.length - 1]);
		Assert.assertEquals(out, Kmer.GENE_CODE.getAdjBit((byte) 'A'));
		result = Kmer.recoverKmerFrom(k, kmer, 0, kmer.length);
		Assert.assertEquals(result, "ATAGAAG");
		
	}

}
