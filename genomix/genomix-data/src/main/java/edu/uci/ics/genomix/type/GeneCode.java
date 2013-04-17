package edu.uci.ics.genomix.type;

public class GeneCode {
	public final static byte[] GENE_SYMBOL = { 'A', 'C', 'G', 'T' };
	/**
	 * make sure this 4 ids equal to the sequence id of char in {@GENE_SYMBOL
	 * }
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

	/**
	 * It works for path merge. Merge the kmer by his next, we need to make sure
	 * the @{t} is a single neighbor.
	 * 
	 * @param t
	 *            the neighbor code in BitMap
	 * @return the genecode
	 */
	public static byte getGeneCodeFromBitMap(byte t) {
		switch (t) {
		case 1 << A:
			return A;
		case 1 << C:
			return C;
		case 1 << G:
			return G;
		case 1 << T:
			return T;
		}
		return -1;
	}
	
	public static int countNumberOfBitSet(int i) {
		int c = 0;
		for (; i != 0; c++) {
			i &= i - 1;
		}
		return c;
	}
	
	public static int inDegree(byte bitmap) {
		return countNumberOfBitSet((bitmap >> 4) & 0x0f);
	}

	public static int outDegree(byte bitmap) {
		return countNumberOfBitSet(bitmap & 0x0f);
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
				str.append((char) GENE_SYMBOL[i]);
			}
		}
		str.append('|');
		for (int i = A; i <= T; i++) {
			if ((right & (1 << i)) != 0) {
				str.append((char) GENE_SYMBOL[i]);
			}
		}
		return str.toString();
	}
}
