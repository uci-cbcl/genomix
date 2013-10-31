/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.genomix.hadoop.gage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.Counters;
import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.hadoop.graph.GraphStatistics;
import edu.uci.ics.genomix.minicluster.GenomixClusterManager;
import edu.uci.ics.genomix.minicluster.GenomixClusterManager.ClusterType;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;

@SuppressWarnings("deprecation")
public class GetFastaStatsTest {
    
    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String BINSOURCE_PATH = "src/test/resources/gagedata/GAGETest.fasta";
    private static final String HDFS_PATH = "/gage";
    private static final String RESULT_PATH = "/gageresult";
    private static final String GAGESOURCE = "src/test/resources/expected/gage/gagesrcfortestcase";
    private FileSystem dfs;
    private static GenomixClusterManager manager;
    private static GenomixJobConf conf = new GenomixJobConf(3);
    
    public void writeTestCaseToFile() throws IOException {
        dfs = FileSystem.getLocal(conf);
        Path targetPath = new Path(BINSOURCE_PATH);

        SequenceFile.Writer writer = new SequenceFile.Writer(dfs, conf, targetPath, VKmer.class, Node.class);
        String[] keyList = { "AGCTG", "CTGAC", "GTACT", "TCAGA", "CATCT", "GACTC" };
        String[] valueList = { "AGTCTCTCTCTCTAGACTCTCTCTTTCTAGAGCTCTCAG", "AGTCTCTCTCTCTAGACTCTCTCTTTCTAGAGCTCTCAGCGT",
                "AGTCTCTCTCTCTAGACTCTCTCTTTCT", "AGTCTCTCTCTCTAGACTCTCTCTTTCTATCGA", "ATCGC",
                "CTTTCTAGAGCTCTCTCTCTCTAGACTCTCTCTCTC" };
        VKmer outputKey = new VKmer();
        Node outputValue = new Node();
        VKmer tempInternalKmer = new VKmer();
        for (int i = 0; i < keyList.length; i++) {
            outputKey.setAsCopy(keyList[i]);
            tempInternalKmer.setAsCopy(valueList[i]);
            outputValue.setInternalKmer(tempInternalKmer);
            writer.append(outputKey, outputValue);
        }
        writer.close();
    }

    @Test
    public void Test() throws Exception {
        writeTestCaseToFile();
        startHadoop();
        prepareData();
        GenomixJobConf.setGlobalStaticConstants(conf);
        Counters counters = GraphStatistics.run(HDFS_PATH, RESULT_PATH, conf);
        GraphStatistics.getFastaStatsForGage(RESULT_PATH, counters, conf);
        cleanupHadoop();
        compareWithGageSourceCodeResults(ACTUAL_RESULT_DIR + File.separator + RESULT_PATH + "/gagestatsFasta.txt");
    }

    public void compareWithGageSourceCodeResults(String mapreducePath) throws Exception {
        BufferedReader brMR = new BufferedReader(new FileReader(new File(mapreducePath)));
        BufferedReader brGageSrc = new BufferedReader(new FileReader(GAGESOURCE));
        StringBuffer etMR = new StringBuffer();
        StringBuffer etGageSrc = new StringBuffer();
        String line;
        while ((line = brMR.readLine()) != null) {
            etMR.append(line);
        }
        while ((line = brGageSrc.readLine()) != null) {
            etGageSrc.append(line);
        }
        brMR.close();
        brGageSrc.close();
        String pureTarget = etMR.toString().replaceAll("\\n", "");
        String pureSrc = etGageSrc.toString().replaceAll("\\n", "");
        Assert.assertEquals(pureSrc, pureTarget);
    }

    public void startHadoop() throws Exception {
        conf = new GenomixJobConf(3);
        conf.setBoolean(GenomixJobConf.RUN_LOCAL, true);
        conf.setInt(GenomixJobConf.STATS_MIN_CONTIGLENGTH, 25);
        conf.setInt(GenomixJobConf.STATS_EXPECTED_GENOMESIZE, 150);
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        manager = new GenomixClusterManager(true, conf);
        manager.setNumberOfDataNodesInLocalMiniHDFS(1);
        manager.startCluster(ClusterType.HADOOP);
    }

    private void prepareData() throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        if (dfs.exists(new Path(HDFS_PATH))) {
            dfs.delete(new Path(HDFS_PATH), true);
        }
        if (dfs.exists(new Path(HDFS_PATH + "/"))) {
            dfs.delete(new Path(HDFS_PATH + "/"), true);
        }
        GenomixClusterManager.copyLocalToHDFS(conf, BINSOURCE_PATH, HDFS_PATH);
    }
    
    public void cleanupHadoop() throws Exception {
        manager.stopCluster(ClusterType.HADOOP);
    }
}
