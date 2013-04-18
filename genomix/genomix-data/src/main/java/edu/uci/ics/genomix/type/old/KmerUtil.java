package edu.uci.ics.genomix.type.old;

import java.util.Arrays;

@Deprecated
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
     * Get last kmer from kmer-chain. 
     * e.g. kmerChain is AAGCTA, if k =5, it will
     * return AGCTA
     * @param k
     * @param kInChain
     * @param kmerChain
     * @return LastKmer bytes array
     */
    public static byte[] getLastKmerFromChain(int k, int kInChain,
            byte[] kmerChain, int offset, int length) {
        if (k > kInChain) {
            return null;
        }
        if (k == kInChain) {
            return kmerChain.clone();
        }
        int byteNum = Kmer.getByteNumFromK(k);
        byte[] kmer = new byte[byteNum];

        /** from end to start */
        int byteInChain = length - 1 - (kInChain - k) / 4;
        int posInByteOfChain = ((kInChain - k) % 4) << 1; // *2
        int byteInKmer = byteNum - 1;
        for (; byteInKmer >= 0 && byteInChain > 0; byteInKmer--, byteInChain--) {
            kmer[byteInKmer] = (byte) ((0xff & kmerChain[offset + byteInChain]) >> posInByteOfChain);
            kmer[byteInKmer] |= ((kmerChain[offset + byteInChain - 1] << (8 - posInByteOfChain)));
        }

        /** last kmer byte */
        if (byteInKmer == 0) {
            kmer[0] = (byte) ((kmerChain[offset] & 0xff) >> posInByteOfChain);
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
    public static byte[] getFirstKmerFromChain(int k, int kInChain,
            byte[] kmerChain, int offset, int length) {
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
            kmer[kmer.length - i] = kmerChain[offset + length - i];
        }
        int posInByteOfChain = (k % 4) << 1; // *2
        if (posInByteOfChain == 0) {
            kmer[0] = kmerChain[offset + length - i];
        } else {
            kmer[0] = (byte) (kmerChain[offset + length - i] & ((1 << posInByteOfChain) - 1));
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
    public static byte[] mergeKmerWithNextCode(int k, byte[] kmer, int offset, int length, byte nextCode) {
        int byteNum = length;
        if (k % 4 == 0) {
            byteNum++;
        }
        byte[] mergedKmer = new byte[byteNum];
        for (int i = 1; i <= length; i++) {
            mergedKmer[mergedKmer.length - i] = kmer[offset + length - i];
        }
        if (mergedKmer.length > length) {
            mergedKmer[0] = (byte) (nextCode & 0x3);
        } else {
            mergedKmer[0] = (byte) (kmer[offset] | ((nextCode & 0x3) << ((k % 4) << 1)));
        }
        return mergedKmer;
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
    public static byte[] mergeKmerWithPreCode(int k, byte[] kmer, int offset, int length, byte preCode) {
        int byteNum = length;
        byte[] mergedKmer = null;
        int byteInMergedKmer = 0;
        if (k % 4 == 0) {
            byteNum++;
            mergedKmer = new byte[byteNum];
            mergedKmer[0] = (byte) ((kmer[offset] >> 6) & 0x3);
            byteInMergedKmer++;
        } else {
            mergedKmer = new byte[byteNum];
        }
        for (int i = 0; i < length - 1; i++, byteInMergedKmer++) {
            mergedKmer[byteInMergedKmer] = (byte) ((kmer[offset + i] << 2) | ((kmer[offset + i + 1] >> 6) & 0x3));
        }
        mergedKmer[byteInMergedKmer] = (byte) ((kmer[offset + length - 1] << 2) | (preCode & 0x3));
        return mergedKmer;
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
    public static byte[] mergeTwoKmer(int preK, byte[] kmerPre, int offsetPre, int lengthPre, int nextK,
            byte[] kmerNext, int offsetNext, int lengthNext) {
        int byteNum = Kmer.getByteNumFromK(preK + nextK);
        byte[] mergedKmer = new byte[byteNum];
        int i = 1;
        for (; i <= lengthPre; i++) {
            mergedKmer[byteNum - i] = kmerPre[offsetPre + lengthPre - i];
        }
        if ( i > 1){
            i--;
        }
        if (preK % 4 == 0) {
            for (int j = 1; j <= lengthNext; j++) {
                mergedKmer[byteNum - i - j] = kmerNext[offsetNext + lengthNext - j];
            }
        } else {
            int posNeedToMove = ((preK % 4) << 1);
            mergedKmer[byteNum - i] |= kmerNext[offsetNext + lengthNext - 1] << posNeedToMove;
            for (int j = 1; j < lengthNext; j++) {
                mergedKmer[byteNum - i - j] = (byte) (((kmerNext[offsetNext + lengthNext
                        - j] & 0xff) >> (8 - posNeedToMove)) | (kmerNext[offsetNext + lengthNext
                        - j - 1] << posNeedToMove));
            }
            if ( (nextK % 4) * 2 + posNeedToMove > 8) {
                mergedKmer[0] = (byte) (kmerNext[offsetNext] >> (8 - posNeedToMove));
            }
        }
        return mergedKmer;
    }
    
    /**
     * Safely shifted the kmer forward without change the input kmer
     * e.g. AGCGC shift with T => GCGCT
     * @param k: kmer length
     * @param kmer: input kmer
     * @param afterCode: input genecode 
     * @return new created kmer that shifted by afterCode, the K will not change
     */
    public static byte[] shiftKmerWithNextCode(int k, final byte[] kmer, int offset, int length, byte afterCode){
        byte[] shifted = Arrays.copyOfRange(kmer, offset, offset+length);
        Kmer.moveKmer(k, shifted, Kmer.GENE_CODE.getSymbolFromCode(afterCode));
        return shifted;
    }
    
    /**
     * Safely shifted the kmer backward without change the input kmer
     * e.g. AGCGC shift with T => TAGCG
     * @param k: kmer length
     * @param kmer: input kmer
     * @param preCode: input genecode 
     * @return new created kmer that shifted by preCode, the K will not change
     */
    public static byte[] shiftKmerWithPreCode(int k, final byte[] kmer, int offset, int length, byte preCode){
        byte[] shifted = Arrays.copyOfRange(kmer, offset, offset+length);
        Kmer.moveKmerReverse(k, shifted, Kmer.GENE_CODE.getSymbolFromCode(preCode));
        return shifted;
    }

    public static byte getGeneCodeAtPosition(int pos, int k, final byte[] kmer,
            int offset, int length) {
        if (pos >= k) {
            return -1;
        }
        int posByte = pos / 4;
        int shift = (pos  % 4) << 1;
        return (byte) ((kmer[offset + length - 1 - posByte] >> shift) & 0x3);
    }
}
