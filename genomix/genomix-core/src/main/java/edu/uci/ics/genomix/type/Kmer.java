package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class Kmer implements Writable {
	
	public final static byte[] GENE_SYMBOL = {'A','C','G','T'};
	public final static class GENE_CODE{
		
		public static final byte A=0;
		public static final byte C=1;
		public static final byte G=2;
		public static final byte T=3;
		
		public static byte getCodeFromSymbol(byte ch){
			byte r = 0;
			switch (ch) {
			case 'A':case 'a':
				r = A;
				break;
			case 'C':case 'c':
				r = C;
				break;
			case 'G':case 'g':
				r = G;
				break;
			case 'T':case 't':
				r = T;
				break;
			}
			return r;
		}
		
		public static byte getSymbolFromCode(byte code){
			if (code > 3){
				return '!';
			}
			return GENE_SYMBOL[code];
		}
		
		public static byte getAdjBit(byte t) {
			byte r = 0;
			switch (t) {
			case 'A':case 'a':
				r = 1 << A;
				break;
			case 'C':case 'c':
				r = 1 << C;
				break;
			case 'G':case 'g':
				r = 1 << G;
				break;
			case 'T':case 't':
				r = 1 << T;
				break;
			}
			return r;
		}
	}

	public static final byte LOWBITMASK = 0x03;
	
	public static String recoverKmerFrom(int k, byte [] keyData, int keyStart, int keyLength ) {
		StringBuilder sblder = new StringBuilder();

		int outKmer = 0;
		for ( int i = keyLength-1; i>=0; i--){
			byte last = keyData[keyStart + i];
			for( int j = 0; j < 8; j+=2){
				byte kmer = (byte) ((last >>j) & LOWBITMASK);
				sblder.append((char)GENE_CODE.getSymbolFromCode(kmer));
				if ( ++outKmer > k){
					break;
				}
			}
			if(outKmer >k){
				break;
			}
		}
		return sblder.toString();
	}
	
	public static String recoverAdjacent(byte number){
		int incoming = (number & 0xF0) >> 4;
		int outgoing = number & 0x0F;
		return String.valueOf(incoming) + '|' + String.valueOf(outgoing);
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}
	
	public static void initializeFilter(int k, byte []filter){
		filter[0] = (byte) 0xC0;
		filter[1] = (byte) 0xFC;
		filter[2] = 0;
		filter[3] = 3;
		final int byteNum = (byte) Math.ceil((double) k / 4.0);
		
		int r = byteNum * 8 - 2 * k;
		r = 8 - r;
		for (int i = 0; i < r; i++) {
			filter[2] <<= 1;
			filter[2] |= 1;
		}
		for(int i = 0; i < r-1 ; i++){
			filter[3] <<= 1;
		}
	}
	
	public static byte[] CompressKmer(int k, byte[] array, int start) {
		final int byteNum = (byte) Math.ceil((double) k / 4.0);
		byte[] bytes = new byte[byteNum + 1];
		bytes[0] = (byte) k;

		byte l = 0;
		int count = 0;
		int bcount = 0;

		for (int i = start; i < start+k ; i++) {
			l = (byte) ((l<<2) & 0xFC);
			l |= GENE_CODE.getCodeFromSymbol(array[i]);
			count += 2;
			if (count % 8 == 0 && byteNum - bcount > 1) {
				bytes[byteNum-bcount] = l;
				bcount += 1;
				count = 0;
				l = 0;
			}
			if (byteNum - bcount <= 1){
				break;
			}
		}
		bytes[1] = l;
		return bytes;
	}
	
	public static void MoveKmer(int k, byte[] bytes, byte c, byte filter[]) {
		int i = (byte) Math.ceil((double) k / 4.0);;
		bytes[i] <<= 2;
		bytes[i] &= filter[1];
		i -= 1;
		while (i > 1) {
			byte f = (byte) (bytes[i] & filter[0]);
			f >>= 6;
			f &= 3;
			bytes[i + 1] |= f;
			bytes[i] <<= 2;
			bytes[i] &= filter[1];
			i -= 1;
		}
		bytes[2] |= (byte) (bytes[1]&filter[3]);
		bytes[1] <<=2;
		bytes[1] &= filter[2];
		bytes[1] |= GENE_CODE.getCodeFromSymbol(c);
	}


}
