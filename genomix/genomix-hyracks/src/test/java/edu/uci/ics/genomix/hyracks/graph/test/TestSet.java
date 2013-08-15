package edu.uci.ics.genomix.hyracks.graph.test;

import java.io.File;

public class TestSet {
    
    public final String PREFIX = "src/test/resources/input/sequence/";
    public final String PATHMERGE = "pathmerge";
    public final String TIP = "tip";
    public final String BUBBLE = "bubble";
    public final String SPLITREPEAT = "splitrepeat";
    public final String BRIDGE = "bridge";

    public final String[] SRSET = { "HighSplitRepeat", "MidSplitRepeat", "LowSplitRepeat" };
    public final String[] TIPSET = { "Tips1", "Tips2", "Tips3", "Tips4" };
    public static enum DirType {
        PATHMERGE,
        TIP,
        BUBBLE,
        SPLITREPEAT,
        BRIDGE
    }
    
    private DirType testSet;
    
    @SuppressWarnings("static-access")
    public TestSet(DirType patternType) {
        switch (patternType) {
            case PATHMERGE:
                testSet = patternType.PATHMERGE;
                break;
            case TIP:
                testSet = patternType.TIP;
                break;
            case BUBBLE:
                testSet = patternType.BUBBLE;
                break;
            case SPLITREPEAT:
                testSet = patternType.SPLITREPEAT;
                break;
            case BRIDGE:
                testSet = patternType.BRIDGE;
                break;
        }
    }
    
    public String[] getTestDir() {
        switch (testSet) {
            case PATHMERGE:
                break;
            case TIP:
                return prepend(TIPSET, PREFIX + TIP + File.separator);
            case BUBBLE:
                break;
            case SPLITREPEAT:
                return prepend(SRSET, PREFIX + SPLITREPEAT+ File.separator);
            case BRIDGE:
                break;
        }
        return null;
    }

    private String[] prepend(String[] input, String prepend) {
        String[] output = new String[input.length];
        for (int index = 0; index < input.length; index++) {
            output[index] = prepend + input[index];
        }
        return output;
    }
    
}
