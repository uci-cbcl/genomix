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
    protected String INPUT_GRAPH;
    protected String OUTPUT_GRAPH;
    protected String localPath;

    public void setupTestConf(int kmerLength, int readLength, String inputDir) throws IOException {
        KMER_LENGTH = kmerLength;
        READ_LENGTH = readLength;
        INPUT_GRAPH = "/input" + inputDir;
        OUTPUT_GRAPH = "/output" + inputDir;
        HDFS_PATHS = new ArrayList<String>(Arrays.asList(OUTPUT_GRAPH));
        copyLocalToDFS(INPUT_ROOT + inputDir, INPUT_GRAPH);
    }
    
    @Test
    public void testTwoReads() throws Exception {
        setupTestConf(5, 8, "/graphs/pathmerge/singleread");
        testPathNode();
        testMergeOneIteration();
        testMergeToCompletion();
    }

	//    @Test
    public void testSimpleText() throws Exception {
        setupTestConf(5, 8, "text.txt");
        testPathNode();
        testMergeOneIteration();
//        testMergeToCompletion();
    }
    
    public void testPathNode() throws IOException {
        cleanUpOutput();
        // identify head and tail nodes with pathnode initial
        PathNodeInitial inith4 = new PathNodeInitial();
        inith4.run(INPUT_GRAPH, OUTPUT_GRAPH + "/toMerge", OUTPUT_GRAPH + "/toUpdate", OUTPUT_GRAPH + "/completed", conf);
    }

    public void testMergeOneIteration() throws Exception {
        cleanUpOutput();
        
        MergePathsH4Driver h4 = new MergePathsH4Driver();
        String outputs = h4.run(INPUT_GRAPH, 2, KMER_LENGTH, 1, conf);
        for (String out : outputs.split(",")) {
            copyResultsToLocal(out, out.replaceFirst("/input/", ACTUAL_ROOT), false, conf);
        }
    }
    
    public void testMergeToCompletion() throws Exception {
    	cleanUpOutput();
        
        MergePathsH4Driver h4 = new MergePathsH4Driver();
        String outputs = h4.run(INPUT_GRAPH, 2, KMER_LENGTH, 50, conf);
        for (String out : outputs.split(",")) {
            copyResultsToLocal(out, out.replaceFirst("/input/", ACTUAL_ROOT), false, conf);
        }
	}
}
