package edu.uci.ics.genomix.pregelix.JobRun.main;

import junit.framework.Test;
import edu.uci.ics.genomix.pregelix.JobRun.BasicGraphCleanTestSuite;

public class BubbleMergeTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "BubbleMerge";
        String testSet[] = { "TwoSimilarInTwo", "ThreeSimilarInThree", "TwoSimilarInThree", "TwoSimilarInFour" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
