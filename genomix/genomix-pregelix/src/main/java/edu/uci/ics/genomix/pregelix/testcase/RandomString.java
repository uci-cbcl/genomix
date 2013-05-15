package edu.uci.ics.genomix.pregelix.testcase;

import java.util.ArrayList;
import java.util.Random;

public class RandomString {

    private static final char[] symbols = new char[4];

    static {
        symbols[0] = 'A';
        symbols[1] = 'C';
        symbols[2] = 'G';
        symbols[3] = 'T';
    }

    private final Random random = new Random();

    private char[] buf;

    private ArrayList<String> existKmer = new ArrayList<String>();;

    private int k;

    public RandomString(int k, int length) {
        if (length < 1)
            throw new IllegalArgumentException("length < 1: " + length);
        buf = new char[length];
        this.k = k;
    }

    public String nextString(int startIdx) {
        String tmp = "";
        for (int idx = startIdx; idx < buf.length;) {
            buf[idx] = symbols[random.nextInt(4)];
            if (idx >= k - 1) {
                tmp = new String(buf, idx - k + 1, k);
                if (!existKmer.contains(tmp)) {
                    existKmer.add(tmp);
                    idx++;
                }
            } else
                idx++;
        }

        return new String(buf);
    }

    public void setLength(int length) {
        buf = new char[length];
    }

    public void addString(String s) {
        char[] tmp = s.toCharArray();
        for (int i = 0; i < tmp.length; i++)
            buf[i] = tmp[i];
    }

}
