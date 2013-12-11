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

package edu.uci.ics.genomix.data.types;

import java.util.Random;

public class RandomTestHelper {
    private static final char[] symbols = new char[4];
    static {
        symbols[0] = 'A';
        symbols[1] = 'C';
        symbols[2] = 'G';
        symbols[3] = 'T';
    }

    public static String generateGeneString(int length) {
        Random random = new Random();
        char[] buf = new char[length];
        for (int idx = 0; idx < buf.length; idx++) {
            buf[idx] = symbols[random.nextInt(4)];
        }
        return new String(buf);
    }

    /**
     * generate a random int within the range [min, max]
     * 
     * @param min
     * @param max
     * @return
     */
    public static int genRandomInt(int min, int max) {
        return min + (int) (Math.random() * ((max - min) + 1));
    }

    /**
     * eg. GCTA will convert to TAGC
     * 
     * @param src
     * @return
     */
    public static String getFlippedGeneStr(String src) {
        int length = src.length();
        char[] input = new char[length];
        char[] output = new char[length];
        src.getChars(0, length, input, 0);
        for (int i = length - 1; i >= 0; i--) {
            switch (input[i]) {
                case 'A':
                    output[length - 1 - i] = 'T';
                    break;
                case 'C':
                    output[length - 1 - i] = 'G';
                    break;
                case 'G':
                    output[length - 1 - i] = 'C';
                    break;
                case 'T':
                    output[length - 1 - i] = 'A';
                    break;
            }
        }
        return new String(output);
    }
}
