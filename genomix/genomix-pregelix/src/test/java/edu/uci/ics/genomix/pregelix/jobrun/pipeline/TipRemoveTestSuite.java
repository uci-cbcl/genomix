package edu.uci.ics.genomix.pregelix.jobrun.pipeline;

import junit.framework.Test;
import edu.uci.ics.genomix.pregelix.jobrun.BasicGraphCleanTestSuite;

public class TipRemoveTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "TIP_REMOVE";
        String testSet[] = { "RF_Tip" }; // "FR_Tip"
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
