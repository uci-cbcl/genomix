package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class BubbleMergeTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="BubbleMerge";
        String testSet[] = {"CTA_GTA_FR_RF"};
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
