package edu.uci.ics.genomix.type;

import java.util.Arrays;

public class KMPMatch {

    private String string;
    private String pattern;
    private int[] failure;
    private int matchPoint;

    public KMPMatch(String string, String pattern) {
        this.string = string;
        this.pattern = pattern;
        failure = new int[pattern.length()];
        computeFailure();
    }

    private void computeFailure() {
        int i = 0;
        failure[0] = -1;
        for (int j = 1; j < pattern.length(); j++) {
            i = failure[j - 1];
            while (i > 0 && pattern.charAt(j) != pattern.charAt(i + 1)) {
                i = failure[i];
            }
            if (pattern.charAt(j) == pattern.charAt(i + 1)) {
                failure[j] = i + 1;
            } else
                failure[j] = -1;
        }
    }

    public int fastFind() {
        int p = 0;
        int s = 0;
        int patternSize = this.pattern.length();
        int strSize = this.string.length();
        while (p < patternSize && s < strSize) {
            if (this.pattern.charAt(p) == this.string.charAt(s)) {
                p++;
                s++;
            } else if (p == 0) {
                s++;
            } else {
                p = failure[p - 1] + 1;
            }
        }
        if (p < patternSize) {
            return -1;
        } else {
            return s - patternSize;
        }

    }

    public static void main(String[] args) {
        String test = "abacacaba";
        KMPMatch kmpMatch = new KMPMatch("abcaaaabbbabacacaba", test);
        System.out.println(kmpMatch.fastFind());
    }
}
