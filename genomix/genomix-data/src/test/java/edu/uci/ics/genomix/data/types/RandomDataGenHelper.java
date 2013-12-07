package edu.uci.ics.genomix.data.types;

import java.util.Random;

public class RandomDataGenHelper {
    private static final char[] symbols = new char[4];
    static {
        symbols[0] = 'A';
        symbols[1] = 'C';
        symbols[2] = 'G';
        symbols[3] = 'T';
    }

    public static String generateString(int length) {
        Random random = new Random();
        char[] buf = new char[length];
        for (int idx = 0; idx < buf.length; idx++) {
            buf[idx] = symbols[random.nextInt(4)];
        }
        return new String(buf);
    }

    public static int genRandomInt(int min, int max) {
        return min + (int) (Math.random() * ((max - min) + 1));
    }

    public static String getReverseStr(String src) {
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
