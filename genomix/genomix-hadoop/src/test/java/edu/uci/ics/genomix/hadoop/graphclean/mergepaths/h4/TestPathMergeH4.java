package edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h3.MergePathsH3Driver;
import edu.uci.ics.genomix.hadoop.pmcommon.GenomixMiniClusterTest;
import edu.uci.ics.genomix.hadoop.pmcommon.HadoopMiniClusterTest;
import edu.uci.ics.genomix.hadoop.pmcommon.PathNodeInitial;
import edu.uci.ics.genomix.hadoop.velvetgraphbuilding.GraphBuildingDriver;
import edu.uci.ics.genomix.hyracks.driver.Driver.Plan;
import edu.uci.ics.genomix.hyracks.job.GenomixJobConf;

@SuppressWarnings("deprecation")
public class TestPathMergeH4 extends HadoopMiniClusterTest {
    protected final String LOCAL_SEQUENCE_FILE = "src/test/resources/data/webmap/text.txt";
    protected final  String SEQUENCE = "/00-sequence/";
    protected final String GRAPHBUILD = "/01-graphbuild/";
    protected final String MERGED = "/02-pathmerge/";
    
    protected final String ACTUAL = "src/test/resources/actual/";
    
    protected final boolean regenerateGraph = true;
    
    {
        KMER_LENGTH = 5;
        READ_LENGTH = 8;
        HDFS_PATHS = new ArrayList<String>(Arrays.asList(SEQUENCE, GRAPHBUILD, MERGED));
        conf.setInt(GenomixJobConf.KMER_LENGTH, KMER_LENGTH);
        conf.setInt(GenomixJobConf.READ_LENGTH, READ_LENGTH);
    }

    @Test
    public void TestMergeOneIteration() throws Exception {
        cleanUpOutput();
        prepareGraph();
        
        MergePathsH4Driver h4 = new MergePathsH4Driver();
        h4.run(GRAPHBUILD, MERGED, 2, KMER_LENGTH, 5, conf);
        copyResultsToLocal(MERGED, ACTUAL_ROOT + MERGED, false, conf);
    }

//    @Test
    public void testPathNode() throws IOException {
        cleanUpOutput();
        prepareGraph();

        // identify head and tail nodes with pathnode initial
        PathNodeInitial inith4 = new PathNodeInitial();
        inith4.run(GRAPHBUILD, "/toMerge", "/completed", conf);
    }
    
    


    public void buildGraph() throws IOException {
        JobConf buildConf = new JobConf(conf);  // use a separate conf so we don't interfere with other jobs 
        FileInputFormat.setInputPaths(buildConf, SEQUENCE);
        FileOutputFormat.setOutputPath(buildConf, new Path(GRAPHBUILD));
        
        GraphBuildingDriver tldriver = new GraphBuildingDriver();
        tldriver.run(SEQUENCE, GRAPHBUILD, 2, KMER_LENGTH, READ_LENGTH, false, true, HADOOP_CONF_ROOT + "conf.xml");
        
        boolean resultsAreText = false;
        copyResultsToLocal(GRAPHBUILD, ACTUAL_ROOT + GRAPHBUILD, resultsAreText, buildConf);
    }
    
    private void prepareGraph() throws IOException {
        if (regenerateGraph) {
            copyLocalToDFS(LOCAL_SEQUENCE_FILE, SEQUENCE);
            buildGraph();
            copyLocalToDFS(ACTUAL_ROOT + GRAPHBUILD + ".bindir", GRAPHBUILD);
        } else {
            copyLocalToDFS(EXPECTED_ROOT + GRAPHBUILD + ".bindir", GRAPHBUILD);
        }
    }
}
