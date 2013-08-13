package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class PathMergeTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="PathMerge";
        String testSet[] = {"4"};//, "3", "4", "5", "6", "7", "8", "9"};
//                "SimplePath",
//                "ThreeDuplicate",
//                "BridgePath",
//                "CyclePath",
//                "SelfPath",
//                "TreePath"
//                };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
