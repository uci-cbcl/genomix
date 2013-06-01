package edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h3;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.junit.Test;

import edu.uci.ics.genomix.hadoop.pmcommon.GenomixMiniClusterTest;
import edu.uci.ics.genomix.hyracks.driver.Driver.Plan;
import edu.uci.ics.genomix.hyracks.job.GenomixJobConf;
import edu.uci.ics.genomix.type.NodeWritable;

@SuppressWarnings("deprecation")
public class TestPathMergeH3 extends GenomixMiniClusterTest {
    protected String LOCAL_SEQUENCE_FILE = "src/test/resources/data/webmap/text.txt";
    protected String HDFS_SEQUENCE = "/00-sequence/";
    protected String HDFS_GRAPHBUILD = "/01-graphbuild/";
    protected String HDFS_MERGED = "/02-graphmerge/";
    
    protected String GRAPHBUILD_FILE = "result.graphbuild.txt";
    protected String PATHMERGE_FILE = "result.mergepath.txt";
    
    {
        KMER_LENGTH = 5;
        READ_LENGTH = 8;
        HDFS_PATHS = new ArrayList<String>(Arrays.asList(HDFS_SEQUENCE, HDFS_GRAPHBUILD, HDFS_MERGED));
        // we have to specify what kind of keys and values this job has
        key = new NodeWritable(KMER_LENGTH);
        value = NullWritable.get();
    }

    @Test
    public void TestBuildGraph() throws Exception {
        cleanUpOutput();
        copyLocalToDFS(LOCAL_SEQUENCE_FILE, HDFS_SEQUENCE);
        buildGraph();
    }

//    @Test
    public void TestMergeOneIteration() throws Exception {
        cleanUpOutput();
        copyLocalToDFS(LOCAL_SEQUENCE_FILE, HDFS_SEQUENCE);
        buildGraph();
        MergePathsH3Driver h3 = new MergePathsH3Driver();
        h3.run(HDFS_GRAPHBUILD, HDFS_MERGED, 2, KMER_LENGTH, 1, ACTUAL_ROOT + "conf.xml", null);
        copyResultsToLocal(HDFS_MERGED, ACTUAL_ROOT + PATHMERGE_FILE, conf);
    }



    public void buildGraph() throws Exception {
        FileInputFormat.setInputPaths(conf, HDFS_SEQUENCE);
        FileOutputFormat.setOutputPath(conf, new Path(HDFS_GRAPHBUILD));
        conf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_BINARY);
        conf.set(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        driver.runJob(new GenomixJobConf(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
        copyResultsToLocal(HDFS_GRAPHBUILD, ACTUAL_ROOT + GRAPHBUILD_FILE, conf);
    }
}
