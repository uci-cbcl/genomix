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

package edu.uci.ics.genomix.data.utils;

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

    public static byte getPairedGeneCode(byte genecode) {
        if (genecode < 0 || genecode > 3) {
            throw new IllegalArgumentException("Invalid genecode: " + genecode);
        }
        return (byte) (3 - genecode);
    }

    public static byte getPairedCodeFromSymbol(byte ch) {
        return getPairedGeneCode(getCodeFromSymbol(ch));
    }

    public static byte getComplimentSymbolFromSymbol(byte ch) {
        return getSymbolFromCode(getPairedGeneCode(getCodeFromSymbol(ch)));
    }

    public static byte getSymbolFromCode(byte code) {
        if (code > 3 || code < 0) {
            throw new IllegalArgumentException("Invalid genecode");
        }
        return GENE_SYMBOL[code];
    }

    public static String reverseComplement(String kmer) {
        StringBuilder sb = new StringBuilder();
        for (char letter : kmer.toCharArray()) {
            sb.append(complement(letter));
        }
        return sb.reverse().toString();
    }

    public static char complement(char ch) {
        switch (ch) {
            case 'A':
            case 'a':
                return 'T';
            case 'C':
            case 'c':
                return 'G';
            case 'G':
            case 'g':
                return 'C';
            case 'T':
            case 't':
                return 'A';
        }
        throw new RuntimeException("Invalid character given in complement: " + ch);
    }
}
