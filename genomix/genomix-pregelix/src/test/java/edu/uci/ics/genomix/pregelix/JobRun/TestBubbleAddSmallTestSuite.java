/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.genomix.pregelix.JobRun;

import java.io.File;

import junit.framework.Test;

public class TestBubbleAddSmallTestSuite extends BasicGraphCleanTestSuite {
    
    public static void init(){
        PreFix = "data/AddBubbleTestSet"; 
        SufFix = "bin";
        PATH_TO_ONLY = "src/test/resources/only_bubbleadd.txt";
        ACTUAL_RESULT_DIR = "data/actual/bubbleadd";
        HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
        /** Multiple Tests **/
        TestDir.clear();
        TestDir.add(PreFix + File.separator + "5" + File.separator + SufFix);
    }
    
    public static Test suite() throws Exception {
        init();
        BasicGraphCleanTestSuite testSuite_bubbleadd = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite_bubbleadd);
    }
}
