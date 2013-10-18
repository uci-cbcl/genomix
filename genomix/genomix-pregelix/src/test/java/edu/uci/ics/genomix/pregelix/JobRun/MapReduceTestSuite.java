package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class MapReduceTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "MapReduce";
        String testSet[] = { "TwoBubbles" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
