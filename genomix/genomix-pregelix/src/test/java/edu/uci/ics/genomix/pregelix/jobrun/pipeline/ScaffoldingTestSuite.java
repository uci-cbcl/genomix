package edu.uci.ics.genomix.pregelix.jobrun.pipeline;

import edu.uci.ics.genomix.pregelix.jobrun.BasicGraphCleanTestSuite;
import junit.framework.Test;

public class ScaffoldingTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "SCAFFOLD";
        String testSet[] = { "PairedEnd" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
