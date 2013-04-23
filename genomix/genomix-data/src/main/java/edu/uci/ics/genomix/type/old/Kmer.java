/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.genomix.type.old;

@Deprecated
public class Kmer {

    public final static byte[] GENE_SYMBOL = { 'A', 'C', 'G', 'T' };

    public final static class GENE_CODE {

        /**
         * make sure this 4 ids equal to the sequence id of char in {@GENE_SYMBOL}
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
         * It works for path merge.
         * Merge the kmer by his next, we need to make sure the @{t} is a single neighbor.
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

    public static String recoverKmerFrom(int k, byte[] keyData, int keyStart, int keyLength) {
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

    public static int getByteNumFromK(int k) {
        int x = k / 4;
        if (k % 4 != 0) {
            x += 1;
        }
        return x;
    }

    /**
     * Compress Kmer into bytes array AATAG will compress as [0x000G, 0xATAA]
     * 
     * @param kmer
     * @param input
     *            array
     * @param start
     *            position
     * @return initialed kmer array
     */
    public static byte[] compressKmer(int k, byte[] array, int start) {
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
    public static byte moveKmer(int k, byte[] kmer, byte c) {
        int byteNum = kmer.length;
        byte output = (byte) (kmer[byteNum - 1] & 0x03);
        for (int i = byteNum - 1; i > 0; i--) {
            byte in = (byte) (kmer[i - 1] & 0x03);
            kmer[i] = (byte) (((kmer[i] >>> 2) & 0x3f) | (in << 6));
        }
        int pos = ((k - 1) % 4) << 1;
        byte code = (byte) (GENE_CODE.getCodeFromSymbol(c) << pos);
        kmer[0] = (byte) (((kmer[0] >>> 2) & 0x3f) | code);
        return (byte) (1 << output);
    }

    public static byte reverseKmerByte(byte k) {
        int x = (((k >> 2) & 0x33) | ((k << 2) & 0xcc));
        return (byte) (((x >> 4) & 0x0f) | ((x << 4) & 0xf0));
    }

    public static byte[] reverseKmer(int k, byte[] kmer) {
        byte[] reverseKmer = new byte[kmer.length];

        int curPosAtKmer = ((k - 1) % 4) << 1;
        int curByteAtKmer = 0;

        int curPosAtReverse = 0;
        int curByteAtReverse = reverseKmer.length - 1;
        reverseKmer[curByteAtReverse] = 0;
        for (int i = 0; i < k; i++) {
            byte gene = (byte) ((kmer[curByteAtKmer] >> curPosAtKmer) & 0x03);
            reverseKmer[curByteAtReverse] |= gene << curPosAtReverse;
            curPosAtReverse += 2;
            if (curPosAtReverse >= 8) {
                curPosAtReverse = 0;
                reverseKmer[--curByteAtReverse] = 0;
            }
            curPosAtKmer -= 2;
            if (curPosAtKmer < 0) {
                curPosAtKmer = 6;
                curByteAtKmer++;
            }
        }

        return reverseKmer;
    }

    /**
     * Compress Reversed Kmer into bytes array AATAG will compress as
     * [0x000A,0xATAG]
     * 
     * @param kmer
     * @param input
     *            array
     * @param start
     *            position
     * @return initialed kmer array
     */
    public static byte[] compressKmerReverse(int k, byte[] array, int start) {
        final int byteNum = getByteNumFromK(k);
        byte[] bytes = new byte[byteNum];

        byte l = 0;
        int bytecount = 0;
        int bcount = byteNum - 1;
        for (int i = start + k - 1; i >= 0; i--) {
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
    public static byte moveKmerReverse(int k, byte[] kmer, byte c) {
        int pos = ((k - 1) % 4) << 1;
        byte output = (byte) ((kmer[0] >> pos) & 0x03);
        for (int i = 0; i < kmer.length - 1; i++) {
            byte in = (byte) ((kmer[i + 1] >> 6) & 0x03);
            kmer[i] = (byte) ((kmer[i] << 2) | in);
        }
        // (k%4) * 2
        if (k % 4 != 0) {
            kmer[0] &= (1 << ((k % 4) << 1)) - 1;
        }
        kmer[kmer.length - 1] = (byte) ((kmer[kmer.length - 1] << 2) | GENE_CODE.getCodeFromSymbol(c));
        return (byte) (1 << output);
    }

}
