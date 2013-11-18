package edu.uci.ics.genomix.pregelix.jobrun.generic;

import edu.uci.ics.genomix.pregelix.jobrun.BasicGraphCleanTestSuite;
import junit.framework.Test;

public class TipAddTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "TIP_ADD";
        String testSet[] = { "5" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
