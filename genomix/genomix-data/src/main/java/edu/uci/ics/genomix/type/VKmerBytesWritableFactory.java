package edu.uci.ics.genomix.type;

public class VKmerBytesWritableFactory {
	private VKmerBytesWritable kmer;
	
	public VKmerBytesWritableFactory(int k){
		kmer = new VKmerBytesWritable(k);
	}

	/**
	 * Read Kmer from read text into bytes array e.g. AATAG will compress as
	 * [0x000G, 0xATAA]
	 * 
	 * @param k
	 * @param array
	 * @param start
	 */
	public VKmerBytesWritable getKmerByRead(int k, byte[] array, int start) {
		kmer.setByRead(k, array, start);
		return kmer;
	}

	/**
	 * Compress Reversed Kmer into bytes array AATAG will compress as
	 * [0x000A,0xATAG]
	 * 
	 * @param array
	 * @param start
	 */
	public VKmerBytesWritable getKmerByReadReverse(int k, byte[] array, int start) {
		kmer.setByReadReverse(k, array, start);
		return kmer;
	}
	
	/**
	 * Get last kmer from kmer-chain. 
	 * e.g. kmerChain is AAGCTA, if k =5, it will
	 * return AGCTA
	 * @param k
	 * @param kInChain
	 * @param kmerChain
	 * @return LastKmer bytes array
	 */
	public VKmerBytesWritable getLastKmerFromChain(int lastK, final KmerBytesWritable kmerChain) {
		if (lastK > kmerChain.getKmerLength()) {
			return null;
		}
		if (lastK == kmerChain.getKmerLength()) {
			kmer.set(kmerChain);
			return kmer;
		}
		kmer.reset(lastK);

		/** from end to start */
		int byteInChain = kmerChain.getLength() - 1 - (kmerChain.getKmerLength() - lastK) / 4;
		int posInByteOfChain = ((kmerChain.getKmerLength() - lastK) % 4) << 1; // *2
		int byteInKmer = kmer.getLength() - 1;
		for (; byteInKmer >= 0 && byteInChain > 0; byteInKmer--, byteInChain--) {
			kmer.getBytes()[byteInKmer] = (byte) ((0xff & kmerChain.getBytes()[byteInChain]) >> posInByteOfChain);
			kmer.getBytes()[byteInKmer] |= ((kmerChain.getBytes()[byteInChain - 1] << (8 - posInByteOfChain)));
		}

		/** last kmer byte */
		if (byteInKmer == 0) {
			kmer.getBytes()[0] = (byte) ((kmerChain.getBytes()[0] & 0xff) >> posInByteOfChain);
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
	 * @return FirstKmer bytes array
	 */
	public VKmerBytesWritable getFirstKmerFromChain(int firstK, final KmerBytesWritable kmerChain) {
		if (firstK > kmerChain.getKmerLength()) {
			return null;
		}
		if (firstK == kmerChain.getKmerLength()) {
			kmer.set(kmerChain);
			return kmer;
		}
		kmer.reset(firstK);

		int i = 1;
		for (; i < kmer.getLength(); i++) {
			kmer.getBytes()[kmer.getLength() - i] = kmerChain.getBytes()[kmerChain.getLength() - i];
		}
		int posInByteOfChain = (firstK % 4) << 1; // *2
		if (posInByteOfChain == 0) {
			kmer.getBytes()[0] = kmerChain.getBytes()[kmerChain.getLength() - i];
		} else {
			kmer.getBytes()[0] = (byte) (kmerChain.getBytes()[kmerChain.getLength() - i] & ((1 << posInByteOfChain) - 1));
		}
		return kmer;
	}
	
	/**
	 * Merge kmer with next neighbor in gene-code format.
	 * The k of new kmer will increase by 1
	 * e.g. AAGCT merge with A => AAGCTA
	 * @param k :input k of kmer
	 * @param kmer : input bytes of kmer
	 * @param nextCode: next neighbor in gene-code format
	 * @return the merged Kmer, this K of this Kmer is k+1
	 */
	public VKmerBytesWritable mergeKmerWithNextCode(final KmerBytesWritable kmer, byte nextCode) {
		this.kmer.reset(kmer.getKmerLength()+1);
		for (int i = 1; i <= kmer.getLength(); i++) {
			this.kmer.getBytes()[this.kmer.getLength() - i] = kmer.getBytes()[kmer.getLength() - i];
		}
		if (this.kmer.getLength() > kmer.getLength()) {
			this.kmer.getBytes()[0] = (byte) (nextCode & 0x3);
		} else {
			this.kmer.getBytes()[0] = (byte) (kmer.getBytes()[0] | ((nextCode & 0x3) << ((kmer.getKmerLength() % 4) << 1)));
		}
		return this.kmer;
	}
	
	/**
	 * Merge kmer with previous neighbor in gene-code format.
	 * The k of new kmer will increase by 1
	 * e.g. AAGCT merge with A => AAAGCT
	 * @param k :input k of kmer
	 * @param kmer : input bytes of kmer
	 * @param preCode: next neighbor in gene-code format
	 * @return the merged Kmer,this K of this Kmer is k+1
	 */
	public VKmerBytesWritable mergeKmerWithPreCode(final KmerBytesWritable kmer, byte preCode) {
		this.kmer.reset(kmer.getKmerLength()+1);
		int byteInMergedKmer = 0;
		if (kmer.getKmerLength() % 4 == 0) {
			this.kmer.getBytes()[0] = (byte) ((kmer.getBytes()[0] >> 6) & 0x3);
			byteInMergedKmer++;
		}
		for (int i = 0; i < kmer.getLength() - 1; i++, byteInMergedKmer++) {
			this.kmer.getBytes()[byteInMergedKmer] = (byte) ((kmer.getBytes()[i] << 2) | ((kmer.getBytes()[ i + 1] >> 6) & 0x3));
		}
		this.kmer.getBytes()[byteInMergedKmer] = (byte) ((kmer.getBytes()[kmer.getLength() - 1] << 2) | (preCode & 0x3));
		return this.kmer;
	}
	
	/**
	 * Merge two kmer to one kmer
	 * e.g. ACTA + ACCGT => ACTAACCGT
	 * @param preK : previous k of kmer
	 * @param kmerPre : bytes array of previous kmer
	 * @param nextK : next k of kmer
	 * @param kmerNext : bytes array of next kmer
	 * @return merged kmer, the new k is @preK + @nextK
	 */
	public VKmerBytesWritable mergeTwoKmer(final KmerBytesWritable preKmer, final KmerBytesWritable nextKmer) {
		kmer.reset(preKmer.getKmerLength() + nextKmer.getKmerLength());
		int i = 1;
		for (; i <= preKmer.getLength(); i++) {
			kmer.getBytes()[kmer.getLength() - i] = preKmer.getBytes()[preKmer.getLength() - i];
		}
		if ( i > 1){
			i--;
		}
		if (preKmer.getKmerLength() % 4 == 0) {
			for (int j = 1; j <= nextKmer.getLength(); j++) {
				kmer.getBytes()[kmer.getLength() - i - j] = nextKmer.getBytes()[nextKmer.getLength() - j];
			}
		} else {
			int posNeedToMove = ((preKmer.getKmerLength() % 4) << 1);
			kmer.getBytes()[kmer.getLength() - i] |= nextKmer.getBytes()[ nextKmer.getLength() - 1] << posNeedToMove;
			for (int j = 1; j < nextKmer.getLength(); j++) {
				kmer.getBytes()[kmer.getLength() - i - j] = (byte) (((nextKmer.getBytes()[ nextKmer.getLength()
						- j] & 0xff) >> (8 - posNeedToMove)) | (nextKmer.getBytes()[nextKmer.getLength()
						- j - 1] << posNeedToMove));
			}
			if ( nextKmer.getKmerLength() % 4 == 0 || (nextKmer.getKmerLength() % 4) * 2 + posNeedToMove > 8) {
				kmer.getBytes()[0] = (byte) ((0xff & nextKmer.getBytes()[0] )>> (8 - posNeedToMove));
			}
		}
		return kmer;
	}
	
	/**
	 * Safely shifted the kmer forward without change the input kmer
	 * e.g. AGCGC shift with T => GCGCT
	 * @param k: kmer length
	 * @param kmer: input kmer
	 * @param afterCode: input genecode 
	 * @return new created kmer that shifted by afterCode, the K will not change
	 */
	public VKmerBytesWritable shiftKmerWithNextCode(final KmerBytesWritable kmer, byte afterCode){
		this.kmer.set(kmer);
		this.kmer.shiftKmerWithNextCode(afterCode);
		return this.kmer;
	}
	
	/**
	 * Safely shifted the kmer backward without change the input kmer
	 * e.g. AGCGC shift with T => TAGCG
	 * @param k: kmer length
	 * @param kmer: input kmer
	 * @param preCode: input genecode 
	 * @return new created kmer that shifted by preCode, the K will not change
	 */
	public VKmerBytesWritable shiftKmerWithPreCode(final KmerBytesWritable kmer, byte preCode){
		this.kmer.set(kmer);
		this.kmer.shiftKmerWithPreCode(preCode);
		return this.kmer;
	}
	
	/**
	 * get the reverse sequence of given kmer
	 * @param kmer
	 */
	public VKmerBytesWritable reverse(final KmerBytesWritable kmer) {
		this.kmer.reset(kmer.getKmerLength());

		int curPosAtKmer = ((kmer.getKmerLength() - 1) % 4) << 1;
		int curByteAtKmer = 0;

		int curPosAtReverse = 0;
		int curByteAtReverse = this.kmer.getLength() - 1;
		this.kmer.getBytes()[curByteAtReverse] = 0;
		for (int i = 0; i < kmer.getKmerLength(); i++) {
			byte gene = (byte) ((kmer.getBytes()[curByteAtKmer] >> curPosAtKmer) & 0x03);
			this.kmer.getBytes()[curByteAtReverse] |= gene << curPosAtReverse;
			curPosAtReverse += 2;
			if (curPosAtReverse >= 8) {
				curPosAtReverse = 0;
				this.kmer.getBytes()[--curByteAtReverse] = 0;
			}
			curPosAtKmer -= 2;
			if (curPosAtKmer < 0) {
				curPosAtKmer = 6;
				curByteAtKmer++;
			}
		}
		return this.kmer;
	}
}
