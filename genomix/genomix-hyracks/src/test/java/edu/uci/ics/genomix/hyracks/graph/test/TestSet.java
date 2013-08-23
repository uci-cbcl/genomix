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
package edu.uci.ics.genomix.hyracks.graph.test;

import java.io.File;
import java.io.IOException;

public class TestSet {

    public final String PREFIX = "src/test/resources/input/sequence/";
    public final String PATHMERGE = "pathmerge";
    public final String TIP = "tip";
    public final String BUBBLE = "bubble";
    public final String SPLITREPEAT = "splitrepeat";
    public final String BRIDGE = "bridge";
    public final String RANDOM = "random";
    
    public final String[] SRSET = { "HighSplitRepeat", "MidSplitRepeat", "LowSplitRepeat" };
    public final String[] TIPSET = { "Tips1", "Tips2", "Tips3", "Tips4" };

    public static enum DirType {
        PATHMERGE,
        TIP,
        BUBBLE,
        SPLITREPEAT,
        BRIDGE,
        RANDOM
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
            case RANDOM:
                testSet = patternType.RANDOM;
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
                return prepend(SRSET, PREFIX + SPLITREPEAT + File.separator);
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

    public String[] getAllTestInputinDir() throws IOException {
        switch (testSet) {
            case PATHMERGE:
                return detectAllTestSet(PREFIX + PATHMERGE);
            case TIP:
                return detectAllTestSet(PREFIX + TIP);
            case BUBBLE:
                break;
            case SPLITREPEAT:
                return detectAllTestSet(PREFIX + SPLITREPEAT);
            case BRIDGE:
                break;
            case RANDOM:
                return detectAllTestSet(PREFIX + RANDOM);
        }
        return null;
    }

    private String[] detectAllTestSet(String inputPrefix) throws IOException {
        File src = new File(inputPrefix);
        String[] output = new String[src.listFiles().length - 1];
        int i = 0;
        for (File f : src.listFiles()) {
            if (!f.getName().contains(".DS_Store"))
                output[i++] = f.getPath().toString();
        }
        System.out.println(output.length);
        return output;
    }
}
