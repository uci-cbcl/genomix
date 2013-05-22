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
package edu.uci.ics.genomix.hadoop.pathmergingh1;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import edu.uci.ics.genomix.hadoop.pathmergingh1.MergePathH1Driver;
import edu.uci.ics.genomix.hadoop.pmcommon.MergePathValueWritable;
import edu.uci.ics.genomix.hadoop.utils.TestUtils;
import edu.uci.ics.genomix.hadoop.oldtype.*;
@SuppressWarnings("deprecation")
public class MergePathH1Test {
    private static final String ACTUAL_RESULT_DIR = "actual3";
    private static final String COMPARE_DIR = "compare";
    private JobConf conf = new JobConf();
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private static final String DATA_PATH = "actual2" + "/result2" + "/part-00000";
    private static final String HDFS_PATH = "/hdfsdata";
    private static final String HDFS_PATH_MERGED = "/pathmerged";
    
    private static final String RESULT_PATH = "/result3";
//    private static final String EXPECTED_PATH = "expected/result3";
    private static final String TEST_SOURCE_DIR = COMPARE_DIR + RESULT_PATH;
    
    private static final int COUNT_REDUCER = 1;
    private static final int SIZE_KMER = 3;
    private static final int MERGE_ROUND = 2;
    
    private MiniDFSCluster dfsCluster;
    private MiniMRCluster mrCluster;
    private FileSystem dfs;

    @SuppressWarnings("resource")
    @Test
    public void test() throws Exception {
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHadoop();

        MergePathH1Driver tldriver = new MergePathH1Driver();
        tldriver.run(HDFS_PATH, RESULT_PATH, HDFS_PATH_MERGED, COUNT_REDUCER, SIZE_KMER, MERGE_ROUND, HADOOP_CONF_PATH);
        
        SequenceFile.Reader reader = null;
        Path path = new Path(HDFS_PATH_MERGED + "/comSinglePath2" + "/comSinglePath2-r-00000");
        reader = new SequenceFile.Reader(dfs, path, conf);
        VKmerBytesWritable key = (VKmerBytesWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        MergePathValueWritable value = (MergePathValueWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        File filePathTo = new File(TEST_SOURCE_DIR);
        FileUtils.forceMkdir(filePathTo);
        FileUtils.cleanDirectory(filePathTo);
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(TEST_SOURCE_DIR + "/comparesource.txt")));
        while (reader.next(key, value)) {
            bw.write(key.toString() + "\t" + value.getAdjBitMap() + "\t" + value.getFlag());
            bw.newLine();
        }
        bw.close();
        
        cleanupHadoop();

    }
    private void startHadoop() throws IOException {
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, 2, true, null);
        dfs = dfsCluster.getFileSystem();
        mrCluster = new MiniMRCluster(4, dfs.getUri().toString(), 2);

        Path src = new Path(DATA_PATH);
        Path dest = new Path(HDFS_PATH + "/");
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(src, dest);
        Path data = new Path(HDFS_PATH_MERGED + "/");
        dfs.mkdirs(data);
   
        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF_PATH)));
        conf.writeXml(confOutput);
        confOutput.flush();
        confOutput.close();
    }

    private void cleanupHadoop() throws IOException {
        mrCluster.shutdown();
        dfsCluster.shutdown();
    }

    private void dumpResult() throws IOException {
        Path src = new Path(RESULT_PATH);
        Path dest = new Path(ACTUAL_RESULT_DIR + "/");
        dfs.copyToLocalFile(src, dest);
    }
}
