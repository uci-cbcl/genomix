package edu.uci.ics.genomix.pregelix.jobrun.pipeline;

import junit.framework.Test;
import edu.uci.ics.genomix.pregelix.jobrun.BasicGraphCleanTestSuite;

public class SplitRepeatTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "SplitRepeat";
        String testSet[] = { "SplitOnce", "SplitTwice", "2to1", "3to1", "3to2", "SimpleSplitManyTimes",
                "ComplexSplitManyTimes", "AdjSplitRepeat_1to1", "AdjSplitRepeat_2to2", "AdjSplitRepeat_3to3",
                "MultiTandemRepeat2" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
