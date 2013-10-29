package edu.uci.ics.genomix.pregelix.jobrun.pipeline;

import edu.uci.ics.genomix.pregelix.jobrun.BasicGraphCleanTestSuite;
import junit.framework.Test;

public class TipRemoveTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "TipRemove";
        String testSet[] = { "FR_Tip", "RF_Tip" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
