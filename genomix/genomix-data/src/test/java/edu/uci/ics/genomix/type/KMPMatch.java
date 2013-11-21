package edu.uci.ics.genomix.type;

import java.util.Arrays;

public class KMPMatch {

    private String string;
    private String pattern;
    private int[] failure;
    private int matchPoint;

    public  static void computeFailure(String pattern, int[] failure) {
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
    
    
    public static void main(String[] args){
        KMPMatch kmpMatch = new KMPMatch();
        String test = "abacacaba";
        int[] failure = new int[test.length()];
        computeFailure(test, failure);
        System.out.println(Arrays.toString(failure));
    }
}
