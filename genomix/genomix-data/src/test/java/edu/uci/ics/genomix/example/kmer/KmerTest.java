package edu.uci.ics.genomix.example.kmer;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;

public class KmerTest {
	static byte[] array = { 'A', 'A', 'T', 'A', 'G', 'A', 'A', 'G' };
	static int k = 7;
	
	@Test
	public void TestCompressKmer() {
		KmerBytesWritable kmer = new KmerBytesWritable(k);
		kmer.setByRead(k, array, 0);
		Assert.assertEquals(kmer.toString(), "AATAGAA");
		
		kmer.setByRead(k, array, 1);
		Assert.assertEquals(kmer.toString(), "ATAGAAG");
	}
	
	@Test
	public void TestMoveKmer(){
		KmerBytesWritable kmer = new KmerBytesWritable(k);
		kmer.setByRead(k, array, 0);
		Assert.assertEquals(kmer.toString(), "AATAGAA");
		
		for (int i = k; i < array.length-1; i++) {
			kmer.shiftKmerWithNextCode(array[i]);
			Assert.assertTrue(false);
		}

		byte out = kmer.shiftKmerWithNextChar( array[array.length - 1]);
		Assert.assertEquals(out, GeneCode.getAdjBit((byte) 'A'));
		Assert.assertEquals(kmer.toString(), "ATAGAAG");
	}
	
	
	@Test
	public void TestCompressKmerReverse() {
		KmerBytesWritable kmer = new KmerBytesWritable(k);
		kmer.setByRead(k, array, 0);
		Assert.assertEquals(kmer.toString(), "AATAGAA");
		
		kmer.setByReadReverse(k, array, 1);
		Assert.assertEquals(kmer.toString(), "GAAGATA");
	}
	
	@Test
	public void TestMoveKmerReverse(){
		KmerBytesWritable kmer = new KmerBytesWritable(k);
		kmer.setByRead(k, array, 0);
		Assert.assertEquals(kmer.toString(), "AATAGAA");
		
		for (int i = k; i < array.length-1; i++) {
			kmer.shiftKmerWithPreChar( array[i]);
			Assert.assertTrue(false);
		}

		byte out = kmer.shiftKmerWithPreChar(array[array.length - 1]);
		Assert.assertEquals(out, GeneCode.getAdjBit((byte) 'A'));
		Assert.assertEquals(kmer.toString(), "GAATAGA");
	}

	@Test
	public void TestGetGene(){
		KmerBytesWritable kmer = new KmerBytesWritable(k);
		String text = "AGCTGACCG";
		byte[] array = { 'A', 'G', 'C', 'T', 'G', 'A', 'C', 'C','G' };
		kmer.setByRead(9, array, 0);
		
		for(int i =0; i < 9; i++){
			Assert.assertEquals(text.charAt(i), 
					(char)(GeneCode.getSymbolFromCode(kmer.getGeneCodeAtPosition(i))));
		}
	}

}
