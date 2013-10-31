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
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.hadoop.graph.GraphStatistics;
import edu.uci.ics.genomix.hadoop.graphbuilding.checkingtool.ResultsCheckingDriver;
import edu.uci.ics.genomix.minicluster.GenomixClusterManager;
//import edu.uci.ics.genomix.hadoop.pmcommon.HadoopMiniClusterTest;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;

public class GetFastaStatsTest {
    private JobConf conf = new JobConf();
    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String BINSOURCE_PATH = "GAGETest.fasta";
    private static final String TXTSOURCE_PATH = "gageTxt.fasta";
    private static final String HDFS_PATH = "/gage";
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private static final String RESULT_PATH = "/gageresult";
    private static final String GAGESOURCE = "data/gage/expected/gagesrcfortestcase";

    private static final int COUNT_REDUCER = 4;
    private MiniDFSCluster dfsCluster;
    private MiniMRCluster mrCluster;
    private FileSystem dfs;

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

    @SuppressWarnings("deprecation")
    @Test
    public void Test() throws Exception {
        writeTestCaseToFile();
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHadoop();
        JobConf conf = new JobConf(HADOOP_CONF_PATH);
        conf.setInt(GenomixJobConf.STATS_MIN_CONTIGLENGTH, 25);
        conf.setInt(GenomixJobConf.STATS_EXPECTED_GENOMESIZE, 150);

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

    public void startHadoop() throws IOException {
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, 1, true, null);
        dfs = dfsCluster.getFileSystem();
        mrCluster = new MiniMRCluster(1, dfs.getUri().toString(), 1);

        Path src = new Path(BINSOURCE_PATH);
        Path dest = new Path(HDFS_PATH + "/");
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(src, dest);

        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF_PATH)));
        conf.writeXml(confOutput);
        confOutput.flush();
        confOutput.close();
    }

    public void cleanupHadoop() throws IOException {
        mrCluster.shutdown();
        dfsCluster.shutdown();
    }
}
