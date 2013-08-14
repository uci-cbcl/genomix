package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class BridgeAddTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="BridgeAdd";
        String testSet[] = {"TwoLines"};
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
