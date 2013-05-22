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

package edu.uci.ics.genomix.hadoop.oldtype;

public class KmerUtil {
    public static final String empty = "";

    public static int getByteNumFromK(int k) {
        int x = k / 4;
        if (k % 4 != 0) {
            x += 1;
        }
        return x;
    }

    public static byte reverseKmerByte(byte k) {
        int x = (((k >> 2) & 0x33) | ((k << 2) & 0xcc));
        return (byte) (((x >> 4) & 0x0f) | ((x << 4) & 0xf0));
    }

    public static String recoverKmerFrom(int k, byte[] keyData, int keyStart, int keyLength) {
        StringBuilder strKmer = new StringBuilder();
        int byteId = keyStart + keyLength - 1;
        if (byteId < 0) {
            return empty;
        }
        byte currentbyte = keyData[byteId];
        for (int geneCount = 0; geneCount < k; geneCount++) {
            if (geneCount % 4 == 0 && geneCount > 0) {
                currentbyte = keyData[--byteId];
            }
            strKmer.append((char) GeneCode.GENE_SYMBOL[(currentbyte >> ((geneCount % 4) * 2)) & 0x03]);
        }
        return strKmer.toString();
    }

}
