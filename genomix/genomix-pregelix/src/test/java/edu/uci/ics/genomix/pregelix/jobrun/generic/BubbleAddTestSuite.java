package edu.uci.ics.genomix.pregelix.jobrun.generic;

import junit.framework.Test;
import edu.uci.ics.genomix.pregelix.jobrun.BasicGraphCleanTestSuite;

public class BubbleAddTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "BUBBLE_ADD";
        String testSet[] = { "3", "TwoSimilarInTwo", "TwoSimilarInThree" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
