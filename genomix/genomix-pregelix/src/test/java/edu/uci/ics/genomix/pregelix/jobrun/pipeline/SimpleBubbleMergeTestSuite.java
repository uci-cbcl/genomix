package edu.uci.ics.genomix.pregelix.jobrun.pipeline;

import junit.framework.Test;
import edu.uci.ics.genomix.pregelix.jobrun.BasicGraphCleanTestSuite;

public class SimpleBubbleMergeTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "BUBBLE";
        String testSet[] = { "SimpleBubble" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
