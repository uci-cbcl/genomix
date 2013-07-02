package edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import edu.uci.ics.genomix.hadoop.pmcommon.HadoopMiniClusterTest;
import edu.uci.ics.genomix.hadoop.pmcommon.PathNodeInitial;
import edu.uci.ics.genomix.hadoop.velvetgraphbuilding.GraphBuildingDriver;

@SuppressWarnings("deprecation")
public class TestPathMergeH4 extends HadoopMiniClusterTest {
    protected String SEQUENCE = "/sequence/";
    protected String GRAPHBUILD = "/graphbuild-unmerged/";
    protected String MERGED = "/pathmerge/";
    protected String LOCAL_SEQUENCE_FILE;
    protected String readsFile;
    protected boolean regenerateGraph;
    {
        HDFS_PATHS = new ArrayList<String>(Arrays.asList(SEQUENCE, GRAPHBUILD, MERGED));
    }
    
    public void setupTestConf(int kmerLength, int readLength, boolean regenerateGraph, String readsFile) {
        KMER_LENGTH = kmerLength;
        READ_LENGTH = readLength;
        this.readsFile = readsFile;
        this.regenerateGraph = regenerateGraph;
        LOCAL_SEQUENCE_FILE = DATA_ROOT + SEQUENCE + readsFile;
    }
    
    @Test
    public void testTwoReads() throws Exception {
        setupTestConf(5, 8, false, "tworeads.txt");
        testPathNode();
//        testMergeOneIteration();
//        testMergeToCompletion();
    }
    
//    @Test
    public void testSimpleText() throws Exception {
        setupTestConf(5, 8, false, "text.txt");
        testPathNode();
        testMergeOneIteration();
//        testMergeToCompletion();
    }
    
    public void testPathNode() throws IOException {
        cleanUpOutput();
        prepareGraph();

        // identify head and tail nodes with pathnode initial
        PathNodeInitial inith4 = new PathNodeInitial();
        inith4.run(GRAPHBUILD, "/toMerge", "/toUpdate", "/completed", conf);
        copyResultsToLocal("/toMerge", ACTUAL_ROOT + "path_toMerge", false, conf);
        copyResultsToLocal("/toUpdate", ACTUAL_ROOT + "path_toUpdate", false, conf);
        copyResultsToLocal("/completed", ACTUAL_ROOT + "path_completed", false, conf);
    }

    public void testMergeOneIteration() throws Exception {
        cleanUpOutput();
        prepareGraph();
        
        MergePathsH4Driver h4 = new MergePathsH4Driver();
        String outputs = h4.run(GRAPHBUILD, 2, KMER_LENGTH, 1, conf);
        int i=0;
        for (String out : outputs.split(",")) {
            copyResultsToLocal(out, ACTUAL_ROOT + MERGED + i++, false, conf);
        }
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
