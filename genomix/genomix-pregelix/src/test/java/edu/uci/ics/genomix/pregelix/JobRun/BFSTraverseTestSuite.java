package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class BFSTraverseTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="BFSTraverse";
        String testSet[] = {"4"};
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
