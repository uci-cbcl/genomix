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
    
    public static int genRandomInt(int min, int max){
        return min + (int) (Math.random() * ((max - min) + 1));
    }
}
