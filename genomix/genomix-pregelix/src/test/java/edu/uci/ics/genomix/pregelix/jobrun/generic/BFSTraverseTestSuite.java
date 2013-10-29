package edu.uci.ics.genomix.pregelix.jobrun.generic;

import edu.uci.ics.genomix.pregelix.jobrun.BasicGraphCleanTestSuite;
import junit.framework.Test;

public class BFSTraverseTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "BFSTraverse";
        String testSet[] = { "4" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
