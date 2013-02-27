package edu.uci.ics.genomix.type;

public class Kmer {

	public final static byte[] GENE_SYMBOL = { 'A', 'C', 'G', 'T' };

	public final static class GENE_CODE {

		/**
		 * make sure this 4 ids equal to the sequence id of char in
		 * {@GENE_SYMBOL}
		 */
		public static final byte A = 0;
		public static final byte C = 1;
		public static final byte G = 2;
		public static final byte T = 3;

		public static byte getCodeFromSymbol(byte ch) {
			byte r = 0;
			switch (ch) {
			case 'A':
			case 'a':
				r = A;
				break;
			case 'C':
			case 'c':
				r = C;
				break;
			case 'G':
			case 'g':
				r = G;
				break;
			case 'T':
			case 't':
				r = T;
				break;
			}
			return r;
		}

		public static byte getSymbolFromCode(byte code) {
			if (code > 3) {
				return '!';
			}
			return GENE_SYMBOL[code];
		}

		public static byte getAdjBit(byte t) {
			byte r = 0;
			switch (t) {
			case 'A':
			case 'a':
				r = 1 << A;
				break;
			case 'C':
			case 'c':
				r = 1 << C;
				break;
			case 'G':
			case 'g':
				r = 1 << G;
				break;
			case 'T':
			case 't':
				r = 1 << T;
				break;
			}
			return r;
		}

		public static byte mergePreNextAdj(byte pre, byte next) {
			return (byte) (pre << 4 | (next & 0x0f));
		}

		public static String getSymbolFromBitMap(byte code) {
			int left = (code >> 4) & 0x0F;
			int right = code & 0x0F;
			StringBuilder str = new StringBuilder();
			for (int i = A; i <= T; i++) {
				if ((left & (1 << i)) != 0) {
					str.append((char)GENE_SYMBOL[i]);
				}
			}
			str.append('|');
			for (int i = A; i <= T; i++) {
				if ((right & (1 << i)) != 0) {
					str.append((char)GENE_SYMBOL[i]);
				}
			}
			return str.toString();
		}
	}

	public static String recoverKmerFrom(int k, byte[] keyData, int keyStart,
			int keyLength) {
		StringBuilder strKmer = new StringBuilder();
		int byteId = keyStart + keyLength - 1;
		byte currentbyte = keyData[byteId];
		for (int geneCount = 0; geneCount < k; geneCount++) {
			if (geneCount % 4 == 0 && geneCount > 0) {
				currentbyte = keyData[--byteId];
			}
			strKmer.append((char) GENE_SYMBOL[(currentbyte >> ((geneCount % 4) * 2)) & 0x03]);
		}
		return strKmer.toString();
	}
	
	public static int getByteNumFromK(int k){
		int x = k/4;
		if (k%4 !=0){
			x+=1;
		}
		return x;
	}

	/**
	 * Compress Kmer into bytes array AATAG will compress as [0 0 0 G][A T A A]
	 * 
	 * @param kmer
	 * @param input
	 *            array
	 * @param start
	 *            position
	 * @return initialed kmer array
	 */
	public static byte[] CompressKmer(int k, byte[] array, int start) {
		final int byteNum = getByteNumFromK(k);
		byte[] bytes = new byte[byteNum];

		byte l = 0;
		int bytecount = 0;
		int bcount = byteNum - 1;
		for (int i = start; i < start + k; i++) {
			byte code = GENE_CODE.getCodeFromSymbol(array[i]);
			l |= (byte) (code << bytecount);
			bytecount += 2;
			if (bytecount == 8) {
				bytes[bcount--] = l;
				l = 0;
				bytecount = 0;
			}
		}
		if (bcount >= 0) {
			bytes[0] = l;
		}
		return bytes;
	}

	/**
	 * Shift Kmer to accept new input
	 * 
	 * @param kmer
	 * @param bytes
	 *            Kmer Array
	 * @param c
	 *            Input new gene character
	 * @return the shiftout gene, in gene code format
	 */
	public static byte MoveKmer(int k, byte[] kmer, byte c) {
		int byteNum = kmer.length;
		byte output = (byte) (kmer[byteNum - 1] & 0x03);
		for (int i = byteNum - 1; i > 0; i--) {
			byte in = (byte) (kmer[i - 1] & 0x03);
			kmer[i] = (byte) (((kmer[i] >>> 2) & 0x3f) | (in << 6));
		}

		int pos = ((k - 1) % 4) * 2;
		byte code = (byte) (GENE_CODE.getCodeFromSymbol(c) << pos);
		kmer[0] = (byte) (((kmer[0] >>> 2) & 0x3f) | code);
		return (byte) (1 << output);
	}

	public static void main(String[] argv) {
		byte[] array = { 'A', 'A', 'T', 'A', 'G', 'A', 'A', 'G' };
		int k = 5;
		byte[] kmer = CompressKmer(k, array, 0);
		for (byte b : kmer) {
			System.out.print(Integer.toBinaryString(b));
			System.out.print(' ');
		}
		System.out.println();
		System.out.println(recoverKmerFrom(k, kmer, 0, kmer.length));

		for (int i = k; i < array.length-1; i++) {
			byte out = MoveKmer(k, kmer, array[i]);

			System.out.println((int) out);
			for (byte b : kmer) {
				System.out.print(Integer.toBinaryString(b));
				System.out.print(' ');
			}
			System.out.println();
			System.out.println(recoverKmerFrom(k, kmer, 0, kmer.length));
		}

		byte out = MoveKmer(k, kmer, array[array.length - 1]);

		System.out.println((int) out);
		for (byte b : kmer) {
			System.out.print(Integer.toBinaryString(b));
			System.out.print(' ');
		}
		System.out.println();
		System.out.println(recoverKmerFrom(k, kmer, 0, kmer.length));

	}

}
