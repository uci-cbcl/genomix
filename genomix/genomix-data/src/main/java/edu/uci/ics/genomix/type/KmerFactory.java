/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.genomix.type;

public class KmerFactory {
    private VKmer kmer;

    public KmerFactory(int k) {
        kmer = new VKmer(k);
    }

    /**
     * Read Kmer from read text into bytes array e.g. AATAG will compress as
     * [0x000G, 0xATAA]
     * 
     * @param k
     * @param array
     * @param start
     */
    public VKmer getKmerByRead(int k, byte[] array, int start) {
        kmer.setFromStringBytes(k, array, start);
        return kmer;
    }

    /**
     * Compress Reversed Kmer into bytes array AATAG will compress as
     * [0x000A,0xATAG]
     * 
     * @param array
     * @param start
     */
    public VKmer getKmerByReadReverse(int k, byte[] array, int start) {
        kmer.setReversedFromStringBytes(k, array, start);
        return kmer;
    }

    /**
     * Get last kmer from kmer-chain.
     * e.g. kmerChain is AAGCTA, if k =5, it will
     * return AGCTA
     * 
     * @param k
     * @param kInChain
     * @param kmerChain
     * @return LastKmer bytes array
     */
    public VKmer getLastKmerFromChain(int lastK, final VKmer kmerChain) {
        if (lastK > kmerChain.getKmerLetterLength()) {
            return null;
        }
        if (lastK == kmerChain.getKmerLetterLength()) {
            kmer.setAsCopy(kmerChain);
            return kmer;
        }
        kmer.reset(lastK);

        /** from end to start */
        int byteInChain = kmerChain.getKmerByteLength() - 1 - (kmerChain.getKmerLetterLength() - lastK) / 4;
        int posInByteOfChain = ((kmerChain.getKmerLetterLength() - lastK) % 4) << 1; // *2
        int byteInKmer = kmer.getKmerByteLength() - 1;
        for (; byteInKmer >= 0 && byteInChain > 0; byteInKmer--, byteInChain--) {
            kmer.getBlockBytes()[byteInKmer + kmer.getKmerOffset()] = (byte) ((0xff & kmerChain.getBlockBytes()[byteInChain
                    + kmerChain.getKmerOffset()]) >> posInByteOfChain);
            kmer.getBlockBytes()[byteInKmer + kmer.getKmerOffset()] |= ((kmerChain.getBlockBytes()[byteInChain
                    + kmerChain.getKmerOffset() - 1] << (8 - posInByteOfChain)));
        }

        /** last kmer byte */
        if (byteInKmer == 0) {
            kmer.getBlockBytes()[0 + kmer.getKmerOffset()] = (byte) ((kmerChain.getBlockBytes()[0 + kmerChain
                    .getKmerOffset()] & 0xff) >> posInByteOfChain);
        }
        kmer.clearLeadBit();
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
    public VKmer getFirstKmerFromChain(int firstK, final VKmer kmerChain) {
        if (firstK > kmerChain.getKmerLetterLength()) {
            return null;
        }
        if (firstK == kmerChain.getKmerLetterLength()) {
            kmer.setAsCopy(kmerChain);
            return kmer;
        }
        kmer.reset(firstK);

        int i = 1;
        for (; i < kmer.getKmerByteLength(); i++) {
            kmer.getBlockBytes()[kmer.getKmerOffset() + kmer.getKmerByteLength() - i] = kmerChain.getBlockBytes()[kmerChain
                    .getKmerOffset() + kmerChain.getKmerByteLength() - i];
        }
        int posInByteOfChain = (firstK % 4) << 1; // *2
        if (posInByteOfChain == 0) {
            kmer.getBlockBytes()[0 + kmer.getKmerOffset()] = kmerChain.getBlockBytes()[kmerChain.getKmerOffset()
                    + kmerChain.getKmerByteLength() - i];
        } else {
            kmer.getBlockBytes()[0 + kmer.getKmerOffset()] = (byte) (kmerChain.getBlockBytes()[kmerChain
                    .getKmerOffset() + kmerChain.getKmerByteLength() - i] & ((1 << posInByteOfChain) - 1));
        }
        kmer.clearLeadBit();
        return kmer;
    }

    public VKmer getSubKmerFromChain(int startK, int kSize, final VKmer kmerChain) {
        if (startK + kSize > kmerChain.getKmerLetterLength()) {
            return null;
        }
        if (startK == 0 && kSize == kmerChain.getKmerLetterLength()) {
            kmer.setAsCopy(kmerChain);
            return kmer;
        }
        kmer.reset(kSize);

        /** from end to start */
        int byteInChain = kmerChain.getKmerByteLength() - 1 - startK / 4;
        int posInByteOfChain = startK % 4 << 1; // *2
        int byteInKmer = kmer.getKmerByteLength() - 1;
        for (; byteInKmer >= 0 && byteInChain > 0; byteInKmer--, byteInChain--) {
            kmer.getBlockBytes()[byteInKmer + kmer.getKmerOffset()] = (byte) ((0xff & kmerChain.getBlockBytes()[byteInChain
                    + kmerChain.getKmerOffset()]) >> posInByteOfChain);
            kmer.getBlockBytes()[byteInKmer + kmer.getKmerOffset()] |= ((kmerChain.getBlockBytes()[byteInChain
                    + kmerChain.getKmerOffset() - 1] << (8 - posInByteOfChain)));
        }

        /** last kmer byte */
        if (byteInKmer == 0) {
            kmer.getBlockBytes()[0 + kmer.getKmerOffset()] = (byte) ((kmerChain.getBlockBytes()[0 + kmerChain
                    .getKmerOffset()] & 0xff) >> posInByteOfChain);
        }
        kmer.clearLeadBit();
        return kmer;
    }

    /**
     * Merge kmer with next neighbor in gene-code format.
     * The k of new kmer will increase by 1
     * e.g. AAGCT merge with A => AAGCTA
     * 
     * @param k
     *            :input k of kmer
     * @param kmer
     *            : input bytes of kmer
     * @param nextCode
     *            : next neighbor in gene-code format
     * @return the merged Kmer, this K of this Kmer is k+1
     */
    public VKmer mergeKmerWithNextCode(final VKmer kmer, byte nextCode) {
        this.kmer.reset(kmer.getKmerLetterLength() + 1);
        for (int i = 1; i <= kmer.getKmerByteLength(); i++) {
            this.kmer.getBlockBytes()[this.kmer.getKmerOffset() + this.kmer.getKmerByteLength() - i] = kmer
                    .getBlockBytes()[kmer.getKmerOffset() + kmer.getKmerByteLength() - i];
        }
        if (this.kmer.getKmerByteLength() > kmer.getKmerByteLength()) {
            this.kmer.getBlockBytes()[0 + kmer.getKmerOffset()] = (byte) (nextCode & 0x3);
        } else {
            this.kmer.getBlockBytes()[0 + kmer.getKmerOffset()] = (byte) (kmer.getBlockBytes()[0 + kmer.getKmerOffset()] | ((nextCode & 0x3) << ((kmer
                    .getKmerLetterLength() % 4) << 1)));
        }
        this.kmer.clearLeadBit();
        return this.kmer;
    }

    /**
     * Merge kmer with previous neighbor in gene-code format.
     * The k of new kmer will increase by 1
     * e.g. AAGCT merge with A => AAAGCT
     * 
     * @param k
     *            :input k of kmer
     * @param kmer
     *            : input bytes of kmer
     * @param preCode
     *            : next neighbor in gene-code format
     * @return the merged Kmer,this K of this Kmer is k+1
     */
    public VKmer mergeKmerWithPreCode(final VKmer kmer, byte preCode) {
        this.kmer.reset(kmer.getKmerLetterLength() + 1);
        int byteInMergedKmer = 0;
        if (kmer.getKmerLetterLength() % 4 == 0) {
            this.kmer.getBlockBytes()[0 + kmer.getKmerOffset()] = (byte) ((kmer.getBlockBytes()[0 + kmer
                    .getKmerOffset()] >> 6) & 0x3);
            byteInMergedKmer++;
        }
        for (int i = 0; i < kmer.getKmerByteLength() - 1; i++, byteInMergedKmer++) {
            this.kmer.getBlockBytes()[byteInMergedKmer + kmer.getKmerOffset()] = (byte) ((kmer.getBlockBytes()[i
                    + kmer.getKmerOffset()] << 2) | ((kmer.getBlockBytes()[i + kmer.getKmerOffset() + 1] >> 6) & 0x3));
        }
        this.kmer.getBlockBytes()[byteInMergedKmer + kmer.getKmerOffset()] = (byte) ((kmer.getBlockBytes()[kmer
                .getKmerOffset() + kmer.getKmerByteLength() - 1] << 2) | (preCode & 0x3));
        this.kmer.clearLeadBit();
        return this.kmer;
    }

    /**
     * Merge two kmer to one kmer
     * e.g. ACTA + ACCGT => ACTAACCGT
     * 
     * @param preK
     *            : previous k of kmer
     * @param kmerPre
     *            : bytes array of previous kmer
     * @param nextK
     *            : next k of kmer
     * @param kmerNext
     *            : bytes array of next kmer
     * @return merged kmer, the new k is @preK + @nextK
     */
    public VKmer mergeTwoKmer(final VKmer preKmer, final VKmer nextKmer) {
        kmer.reset(preKmer.getKmerLetterLength() + nextKmer.getKmerLetterLength());
        int i = 1;
        for (; i <= preKmer.getKmerByteLength(); i++) {
            kmer.getBlockBytes()[kmer.getKmerOffset() + kmer.getKmerByteLength() - i] = preKmer.getBlockBytes()[preKmer
                    .getKmerOffset() + preKmer.getKmerByteLength() - i];
        }
        if (i > 1) {
            i--;
        }
        if (preKmer.getKmerLetterLength() % 4 == 0) {
            for (int j = 1; j <= nextKmer.getKmerByteLength(); j++) {
                kmer.getBlockBytes()[kmer.getKmerOffset() + kmer.getKmerByteLength() - i - j] = nextKmer
                        .getBlockBytes()[nextKmer.getKmerOffset() + nextKmer.getKmerByteLength() - j];
            }
        } else {
            int posNeedToMove = ((preKmer.getKmerLetterLength() % 4) << 1);
            kmer.getBlockBytes()[kmer.getKmerOffset() + kmer.getKmerByteLength() - i] |= nextKmer.getBlockBytes()[nextKmer
                    .getKmerOffset() + nextKmer.getKmerByteLength() - 1] << posNeedToMove;
            for (int j = 1; j < nextKmer.getKmerByteLength(); j++) {
                kmer.getBlockBytes()[kmer.getKmerOffset() + kmer.getKmerByteLength() - i - j] = (byte) (((nextKmer
                        .getBlockBytes()[nextKmer.getKmerOffset() + nextKmer.getKmerByteLength() - j] & 0xff) >> (8 - posNeedToMove)) | (nextKmer
                        .getBlockBytes()[nextKmer.getKmerOffset() + nextKmer.getKmerByteLength() - j - 1] << posNeedToMove));
            }
            if (nextKmer.getKmerLetterLength() % 4 == 0 || (nextKmer.getKmerLetterLength() % 4) * 2 + posNeedToMove > 8) {
                kmer.getBlockBytes()[0 + kmer.getKmerOffset()] = (byte) ((0xff & nextKmer.getBlockBytes()[0 + nextKmer
                        .getKmerOffset()]) >> (8 - posNeedToMove));
            }
        }
        kmer.clearLeadBit();
        return kmer;
    }

    /**
     * Safely shifted the kmer forward without change the input kmer
     * e.g. AGCGC shift with T => GCGCT
     * 
     * @param k
     *            : kmer length
     * @param kmer
     *            : input kmer
     * @param afterCode
     *            : input genecode
     * @return new created kmer that shifted by afterCode, the K will not change
     */
    public VKmer shiftKmerWithNextCode(final VKmer kmer, byte afterCode) {
        this.kmer.setAsCopy(kmer);
        this.kmer.shiftKmerWithNextCode(afterCode);
        return this.kmer;
    }

    /**
     * Safely shifted the kmer backward without change the input kmer
     * e.g. AGCGC shift with T => TAGCG
     * 
     * @param k
     *            : kmer length
     * @param kmer
     *            : input kmer
     * @param preCode
     *            : input genecode
     * @return new created kmer that shifted by preCode, the K will not change
     */
    public VKmer shiftKmerWithPreCode(final VKmer kmer, byte preCode) {
        this.kmer.setAsCopy(kmer);
        this.kmer.shiftKmerWithPreCode(preCode);
        return this.kmer;
    }

    /**
     * get the reverse sequence of given kmer
     * 
     * @param kmer
     */
    public VKmer reverse(final VKmer kmer) {
        this.kmer.reset(kmer.getKmerLetterLength());

        int curPosAtKmer = ((kmer.getKmerLetterLength() - 1) % 4) << 1;
        int curByteAtKmer = 0;

        int curPosAtReverse = 0;
        int curByteAtReverse = this.kmer.getKmerByteLength() - 1;
        this.kmer.getBlockBytes()[curByteAtReverse + this.kmer.getKmerOffset()] = 0;
        for (int i = 0; i < kmer.getKmerLetterLength(); i++) {
            byte gene = (byte) ((kmer.getBlockBytes()[curByteAtKmer + kmer.getKmerOffset()] >> curPosAtKmer) & 0x03);
            this.kmer.getBlockBytes()[curByteAtReverse + this.kmer.getKmerOffset()] |= gene << curPosAtReverse;
            curPosAtReverse += 2;
            if (curPosAtReverse >= 8) {
                curPosAtReverse = 0;
                this.kmer.getBlockBytes()[--curByteAtReverse + this.kmer.getKmerOffset()] = 0;
            }
            curPosAtKmer -= 2;
            if (curPosAtKmer < 0) {
                curPosAtKmer = 6;
                curByteAtKmer++;
            }
        }
        this.kmer.clearLeadBit();
        return this.kmer;
    }
}
