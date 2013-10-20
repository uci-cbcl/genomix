package edu.uci.ics.genomix.pregelix.JobRun.test;

import junit.framework.Test;
import edu.uci.ics.genomix.pregelix.JobRun.BasicGraphCleanTestSuite;

public class BubbleAddTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "BubbleAdd";
        String testSet[] = { "3", "TwoSimilarInTwo", "TwoSimilarInThree" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
