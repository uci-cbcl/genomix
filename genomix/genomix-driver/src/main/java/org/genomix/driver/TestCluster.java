/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package org.genomix.driver;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.genomix.config.GenomixJobConf;
//import edu.uci.ics.hyracks.hdfs.utils.HyracksUtils;
//import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;
//import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;

@SuppressWarnings("deprecation")
public class TestCluster {
    private static final Logger LOGGER = Logger.getLogger(TestCluster.class.getName());

    private static final String CLUSTER_DIR = "tmp_TestCluster";
    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final String PATH_TO_CLUSTER_STORE = "src/test/resources/cluster/stores.properties";
    private static final String PATH_TO_CLUSTER_PROPERTIES = "src/test/resources/cluster/cluster.properties";
    private static final String HADOOP_CONF_PATH = CLUSTER_DIR + File.separator + "conf.xml";
    private MiniDFSCluster dfsCluster;

    public void setUp(GenomixJobConf mainConf) throws Exception {
//        ClusterConfig.setStorePath(PATH_TO_CLUSTER_STORE);
//        ClusterConfig.setClusterPropertiesPath(PATH_TO_CLUSTER_PROPERTIES);
        FileUtils.forceMkdir(new File(CLUSTER_DIR));
        FileUtils.cleanDirectory(new File(CLUSTER_DIR));
        cleanupStores();
        MyHyracksUtils.init();
//        PregelixHyracksIntegrationUtil.init();
        LOGGER.info("Hyracks mini-cluster started");
        startHDFS(mainConf);
    }

    private void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File(CLUSTER_DIR + File.separator + "teststore"));
        FileUtils.forceMkdir(new File(CLUSTER_DIR + File.separator + "build"));
        FileUtils.cleanDirectory(new File(CLUSTER_DIR + File.separator + "teststore"));
        FileUtils.cleanDirectory(new File(CLUSTER_DIR + File.separator + "build"));
    }

    private void startHDFS(GenomixJobConf mainConf) throws IOException {
        mainConf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        mainConf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        mainConf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path(CLUSTER_DIR + File.separator + "build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(mainConf, Integer.parseInt(mainConf.get(GenomixJobConf.CPARTITION_PER_MACHINE)), true, null);

        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF_PATH)));
        mainConf.writeXml(confOutput);
        confOutput.flush();
        confOutput.close();
    }

    /**
     * cleanup hdfs cluster
     */
    private void cleanupHDFS() throws Exception {
        dfsCluster.shutdown();
    }

    public void tearDown() throws Exception {
//        PregelixHyracksIntegrationUtil.deinit();
        MyHyracksUtils.deinit();
        LOGGER.info("Hyracks mini-cluster shut down");
        cleanupHDFS();
    }

}