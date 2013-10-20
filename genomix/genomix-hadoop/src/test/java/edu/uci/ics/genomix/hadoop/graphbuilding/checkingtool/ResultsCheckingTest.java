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
package edu.uci.ics.genomix.hadoop.graphbuilding.checkingtool;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.Test;

import edu.uci.ics.genomix.hadoop.pmcommon.HadoopMiniClusterTest;

/**
 * This test only applied on SequenceInputFormat
 */

@SuppressWarnings("deprecation")
public class ResultsCheckingTest {

    private JobConf conf = new JobConf();
    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private static final String HADOOP_PATH = "checkingrepository/hadoopfile";
    private static final String HYRACKS_PATH = "checkingrepository/hyracksfile";
    private static final String HDFS_HADOOP_PATH = "/hadoop";
    private static final String HDFS_HYRACKS_PATH = "/hyracks";
    private static final String RESULT_PATH = "/checkingresult";

    private static final int COUNT_REDUCER = 2;

    private MiniDFSCluster dfsCluster;
    private MiniMRCluster mrCluster;
    private FileSystem dfs;

    @Test
    public void test() throws Exception {
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHadoop();
        ResultsCheckingDriver driver = new ResultsCheckingDriver();
        driver.run(HDFS_HADOOP_PATH, HDFS_HYRACKS_PATH, RESULT_PATH, COUNT_REDUCER, HADOOP_CONF_PATH);
        dumpResult();
        cleanupHadoop();
    }

    public void startHadoop() throws IOException {
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, 1, true, null);
        dfs = dfsCluster.getFileSystem();
        mrCluster = new MiniMRCluster(1, dfs.getUri().toString(), 1);

        Path src = new Path(HADOOP_PATH);
        Path dest = new Path(HDFS_HADOOP_PATH + "/");
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(src, dest);
        //        System.out.println(dest.toString());
        //        System.out.println(dest.getName());
        //        SequenceFile.Reader reader = null;
        //        Path path = new Path(HDFS_HADOOP_PATH + "/hadoopfile");
        //        reader = new SequenceFile.Reader(dfs, path, conf); 
        //        VKmerBytesWritable key = new VKmerBytesWritable();
        //        NodeWritable value = (NodeWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        //        while(reader.next(key, value)) {
        //            System.out.println(key.toString());
        //            System.out.println(value.toString());
        //        }
        src = new Path(HYRACKS_PATH);
        dest = new Path(HDFS_HYRACKS_PATH + "/");
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

    private void dumpResult() throws IOException {
        Path src = new Path(RESULT_PATH);
        Path dest = new Path(ACTUAL_RESULT_DIR);
        dfs.copyToLocalFile(src, dest);
        HadoopMiniClusterTest.copyResultsToLocal(RESULT_PATH, "actual/test.txt", true, conf, true, dfs);
    }
}
