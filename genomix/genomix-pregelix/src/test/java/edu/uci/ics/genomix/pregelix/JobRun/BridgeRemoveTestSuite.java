package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class BridgeRemoveTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="BridgeRemove";
        String testSet[] = {"GTA_up(FR)_down(RF)"};
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
