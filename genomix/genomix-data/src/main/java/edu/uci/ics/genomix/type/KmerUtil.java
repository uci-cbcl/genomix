package edu.uci.ics.genomix.type;

public class KmerUtil {

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

	/**
	 * Get last kmer from kmer-chain. e.g. kmerChain is AAGCTA, if k =5, it will
	 * return AGCTA
	 * 
	 * @param k
	 * @param kInChain
	 * @param kmerChain
	 * @return
	 */
	public static byte[] getLastKmerFromChain(int k, int kInChain,
			byte[] kmerChain) {
		if (k > kInChain) {
			return null;
		}
		if (k == kInChain) {
			return kmerChain.clone();
		}
		int byteNum = Kmer.getByteNumFromK(k);
		byte[] kmer = new byte[byteNum];

		/** from end to start */
		int byteInChain = kmerChain.length - 1 - (kInChain - k) / 4;
		int posInByteOfChain = ((kInChain - k) % 4) << 1; // *2
		int byteInKmer = byteNum - 1;
		for (; byteInKmer >= 0 && byteInChain > 0; byteInKmer--, byteInChain--) {
			kmer[byteInKmer] = (byte) ((0xff & kmerChain[byteInChain]) >> posInByteOfChain);
			kmer[byteInKmer] |= ((kmerChain[byteInChain - 1] << (8 - posInByteOfChain)));
		}

		/** last kmer byte */
		if (byteInKmer == 0) {
			kmer[0] = (byte) ((kmerChain[0] & 0xff) >> posInByteOfChain);
		}
		return kmer;
	}

	/**
	 * Get first kmer from kmer-chain e.g. kmerChain is AAGCTA, if k=5, it will
	 * return AAGCT
	 * 
	 * @param k
	 * @param kInChain
	 * @param kmerChain
	 * @return
	 */
	public static byte[] getFirstKmerFromChain(int k, int kInChain,
			byte[] kmerChain) {
		if (k > kInChain) {
			return null;
		}
		if (k == kInChain) {
			return kmerChain.clone();
		}
		int byteNum = Kmer.getByteNumFromK(k);
		byte[] kmer = new byte[byteNum];

		int i = 1;
		for (; i < kmer.length; i++) {
			kmer[kmer.length - i] = kmerChain[kmerChain.length - i];
		}
		int posInByteOfChain = (k % 4) << 1; // *2
		if (posInByteOfChain == 0) {
			kmer[0] = kmerChain[kmerChain.length - i];
		} else {
			kmer[0] = (byte) (kmerChain[kmerChain.length - i] & ((1 << posInByteOfChain) - 1));
		}
		return kmer;
	}

	public static byte[] mergeKmerWithNextCode(int k, byte[] kmer, byte nextCode) {
		int byteNum = kmer.length;
		if (k % 4 == 0) {
			byteNum++;
		}
		byte[] mergedKmer = new byte[byteNum];
		for (int i = 1; i <= kmer.length; i++) {
			mergedKmer[mergedKmer.length - i] = kmer[kmer.length - i];
		}
		if (mergedKmer.length > kmer.length) {
			mergedKmer[0] = (byte) (nextCode & 0x3);
		} else {
			mergedKmer[0] = (byte) (kmer[0] | ((nextCode & 0x3) << ((k % 4) << 1)));
		}
		return mergedKmer;
	}

	public static byte[] mergeKmerWithPreCode(int k, byte[] kmer, byte preCode) {
		int byteNum = kmer.length;
		byte[] mergedKmer = null;
		int byteInMergedKmer = 0;
		if (k % 4 == 0) {
			byteNum++;
			mergedKmer = new byte[byteNum];
			mergedKmer[0] = (byte) ((kmer[0] >> 6) & 0x3);
			byteInMergedKmer++;
		} else {
			mergedKmer = new byte[byteNum];
		}
		for (int i = 0; i < kmer.length - 1; i++, byteInMergedKmer++) {
			mergedKmer[byteInMergedKmer] = (byte) ((kmer[i] << 2) | ((kmer[i + 1] >> 6) & 0x3));
		}
		mergedKmer[byteInMergedKmer] = (byte) ((kmer[kmer.length - 1] << 2) | (preCode & 0x3));
		return mergedKmer;
	}

	public static byte[] mergeTwoKmer(int preK, byte[] kmerPre, int nextK,
			byte[] kmerNext) {
		int byteNum = Kmer.getByteNumFromK(preK + nextK);
		byte[] mergedKmer = new byte[byteNum];
		int i = 1;
		for (; i <= kmerPre.length; i++) {
			mergedKmer[byteNum - i] = kmerPre[kmerPre.length - i];
		}
		i--;
		if (preK % 4 == 0) {
			for (int j = 1; j <= kmerNext.length; j++) {
				mergedKmer[byteNum - i - j] = kmerNext[kmerNext.length - j];
			}
		} else {
			int posNeedToMove = ((preK % 4) << 1);
			mergedKmer[byteNum - i] |= kmerNext[kmerNext.length - 1] << posNeedToMove;
			for (int j = 1; j < kmerNext.length; j++) {
				mergedKmer[byteNum - i - j] = (byte) (((kmerNext[kmerNext.length
						- j] & 0xff) >> (8 - posNeedToMove)) | (kmerNext[kmerNext.length
						- j - 1] << posNeedToMove));
			}
			if ( (nextK % 4) * 2 + posNeedToMove > 8) {
				mergedKmer[0] = (byte) (kmerNext[0] >> (8 - posNeedToMove));
			}
		}
		return mergedKmer;
	}

}
