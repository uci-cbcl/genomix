package edu.uci.ics.genomix.pregelix.JobRun.main;

import edu.uci.ics.genomix.pregelix.JobRun.BasicGraphCleanTestSuite;
import junit.framework.Test;

public class ScaffoldingTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "Scaffolding";
        String testSet[] = { "PairedEnd" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
