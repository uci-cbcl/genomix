package edu.uci.ics.genomix.pregelix.jobrun.pipeline;

import junit.framework.Test;
import edu.uci.ics.genomix.pregelix.jobrun.BasicGraphCleanTestSuite;

public class BubbleMergeTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "BUBBLE";
        String testSet[] = { "TwoSimilarInTwo", "ThreeSimilarInThree", "TwoSimilarInThree", "TwoSimilarInFour",
                "SimpleLongReads", "SimpleLongReads3" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
