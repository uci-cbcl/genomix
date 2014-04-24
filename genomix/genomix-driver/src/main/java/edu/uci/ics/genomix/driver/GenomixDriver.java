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

package edu.uci.ics.genomix.driver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.codehaus.plexus.util.FileUtils;
import org.kohsuke.args4j.CmdLineException;

import edu.uci.ics.genomix.data.cluster.DriverUtils;
import edu.uci.ics.genomix.data.cluster.GenomixClusterManager;
import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.config.GenomixJobConf.Patterns;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.utils.GenerateGraphViz;
import edu.uci.ics.genomix.data.utils.GenerateGraphViz.GRAPH_TYPE;
import edu.uci.ics.genomix.hadoop.buildgraph.GenomixHadoopDriver;
import edu.uci.ics.genomix.hadoop.utils.ConvertToFasta;
import edu.uci.ics.genomix.hadoop.utils.GraphStatistics;
import edu.uci.ics.genomix.hyracks.graph.driver.GenomixHyracksDriver;
import edu.uci.ics.genomix.hyracks.graph.driver.GenomixHyracksDriver.Plan;
import edu.uci.ics.genomix.mixture.model.FittingMixture;
import edu.uci.ics.genomix.pregelix.operator.bridgeremove.BridgeRemoveVertex;
import edu.uci.ics.genomix.pregelix.operator.bubblesearch.BubbleSearchVertex;
import edu.uci.ics.genomix.pregelix.operator.extractsubgraph.ExtractSubgraphVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P1ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P4ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.scaffolding2.PruneVertex;
import edu.uci.ics.genomix.pregelix.operator.removebadcoverage.RemoveBadCoverageVertex;
//import edu.uci.ics.genomix.pregelix.operator.removelowcoverage.ShiftLowCoverageReadSetVertex;
import edu.uci.ics.genomix.pregelix.operator.scaffolding2.RayVertex;
import edu.uci.ics.genomix.pregelix.operator.seeddetection.ConfidentVertex;
import edu.uci.ics.genomix.pregelix.operator.seeddetection.SeedRetrievalVertex;
import edu.uci.ics.genomix.pregelix.operator.simplebubblemerge.SimpleBubbleMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.symmetrychecker.SymmetryCheckerVertex;
import edu.uci.ics.genomix.pregelix.operator.test.BridgeAddVertex;
import edu.uci.ics.genomix.pregelix.operator.test.BubbleAddVertex;
import edu.uci.ics.genomix.pregelix.operator.test.TipAddVertex;
import edu.uci.ics.genomix.pregelix.operator.tipremove.SingleNodeTipRemoveVertex;
import edu.uci.ics.genomix.pregelix.operator.tipremove.TipRemoveWithSearchVertex;
import edu.uci.ics.genomix.pregelix.operator.unrolltandemrepeat.UnrollTandemRepeat;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;

/**
 * The main entry point for the Genomix assembler, a hyracks/pregelix/hadoop-based deBruijn assembler.
 */
public class GenomixDriver {

    public static final Logger GENOMIX_ROOT_LOG = Logger.getLogger("edu.uci.ics.genomix"); // here only so we can control children loggers 
    private static final Logger LOG = Logger.getLogger(GenomixDriver.class.getName());
    private String tmpPrevOutput;
    private String tmpCurOutput;
    private Boolean aggressivePrune = false;
    private Boolean tmpOutput = false;
    private String prevOutput;
    private String curOutput;
    private int stepNum;
    private List<PregelixJob> pregelixJobs;
    Counters prevStatsCounters = null;
    private boolean runLocal = false;
    private int threadsPerMachine;
    private int numMachines;

    private GenomixClusterManager manager;
    private GenomixHyracksDriver hyracksDriver;
    private edu.uci.ics.pregelix.core.driver.Driver pregelixDriver;

    @SuppressWarnings("deprecation")
    private void setOutput(GenomixJobConf conf, Patterns step) {
    	if (tmpOutput){
        	tmpPrevOutput = curOutput;
        	tmpCurOutput = conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator + String.format("%02d-", stepNum) + step;
        	FileInputFormat.setInputPaths(conf, new Path(tmpPrevOutput));
            FileOutputFormat.setOutputPath(conf, new Path(tmpCurOutput));
        } else {
        prevOutput = curOutput;
        curOutput = conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator + String.format("%02d-", stepNum) + step;
        FileInputFormat.setInputPaths(conf, new Path(prevOutput));
        FileOutputFormat.setOutputPath(conf, new Path(curOutput));
        }
    }

    public static double cur_expMean = -1;
    public static double cur_normalMean = -1;
    public static double cur_normalStd = -1;

    private void setCutoffCoverageByFittingMixture(GenomixJobConf conf) throws IOException {
        Counters counters = GraphStatistics.run(curOutput, curOutput + "-cov-stats", conf);
        GraphStatistics.drawCoverageStatistics(curOutput + "-cov-stats", counters, conf);
        copyToLocalOutputDir(curOutput + "-cov-stats", conf);

        double maxCoverage = GraphStatistics.getMaxCoverage(counters);
        double[] coverageData = GraphStatistics.getCoverageStats(counters);
        if (maxCoverage == 0 || coverageData.length == 0)
            throw new IllegalStateException("No information for coverage!");
        long cutoffCoverage = (long) FittingMixture.fittingMixture(coverageData, maxCoverage, 10);

        if (cutoffCoverage > 0) {
            LOG.info("Set the cutoffCoverage to " + cutoffCoverage);
            conf.setFloat(GenomixJobConf.REMOVE_BAD_COVERAGE_MIN_COVERAGE, cutoffCoverage);
        } else {
            LOG.info("The generated cutoffCoverage is " + cutoffCoverage + "! It's not set.");
        }
    }

    private void setMinScaffoldingSeedLength(GenomixJobConf conf) throws IOException {
        Counters counters = GraphStatistics.run(curOutput, curOutput + "-kmerLength-stats", conf);
        long totalNodes = counters.getGroup("totals").getCounter("nodes");
        float fraction = 0.01f; // TODO Should it provide by user?
        long numOfSeeds;
        // maximum number of nodes is 100
        if (Math.round((float) totalNodes * fraction) < 100)
            numOfSeeds = (long) Math.round((float) totalNodes * fraction);
        else
            numOfSeeds = 100;

        // sort counters
        ArrayList<Long> sortedCounter = new ArrayList<Long>();
        int maxLength = sortCounters(counters.getGroup("kmerLength-bins"), sortedCounter);
        long curNumOfSeeds = 0;
        for (int i = sortedCounter.size() - 1; i >= 0; i--) {
            if (sortedCounter.get(i) != null)
                curNumOfSeeds += sortedCounter.get(i);
            if (curNumOfSeeds > numOfSeeds) {
                conf.setInt(GenomixJobConf.SCAFFOLDING_SEED_LENGTH_THRESHOLD, maxLength
                        - (sortedCounter.size() - 1 - i));
                return;
            }
        }
        throw new IllegalStateException("It is impossible to reach here!");
    }

    // bucket sort to sort Counters
    private int sortCounters(Group group, ArrayList<Long> sortedCounter) {
        int minLength = 0;
        int maxLength = 0;
        for (Counter c : group) {
            if (minLength > Integer.parseInt(c.getName()))
                minLength = Integer.parseInt(c.getName());
            if (maxLength < Integer.parseInt(c.getName()))
                maxLength = Integer.parseInt(c.getName());
        }

        Long[] sortedCounterArray = new Long[maxLength - minLength + 1];
        for (Counter c : group) {
            sortedCounterArray[Integer.parseInt(c.getName()) - minLength] = c.getValue();
        }

        sortedCounter.clear();
        for (int i = 0; i < sortedCounterArray.length; i++)
            sortedCounter.add(sortedCounterArray[i]);
        return maxLength;
    }

    private void addStep(GenomixJobConf conf, Patterns step) throws Exception {
        // oh, java, why do you pain me so?
        switch (step) {
            case BUILD:
            case BUILD_HYRACKS:
                flushPendingJobs(conf);
                buildGraphWithHyracks(conf);
                if (Boolean.parseBoolean(conf.get(GenomixJobConf.SET_CUTOFF_COVERAGE)))
                    setCutoffCoverageByFittingMixture(conf);
                break;
            case BUILD_HADOOP:
                flushPendingJobs(conf);
                buildGraphWithHadoop(conf);
                break;
            case MERGE_P1:
                pregelixJobs.add(P1ForPathMergeVertex.getConfiguredJob(conf, P1ForPathMergeVertex.class));
                break;
            case MERGE_P2:
                //                queuePregelixJob(P2ForPathMergeVertex.getConfiguredJob(conf, P2ForPathMergeVertex.class));
                //                break;
                throw new UnsupportedOperationException("MERGE_P2 has errors!");
            case MERGE:
            case MERGE_P4:
                pregelixJobs.add(P4ForPathMergeVertex.getConfiguredJob(conf, P4ForPathMergeVertex.class));
                // flushPendingJobs(conf);
                // setMinScaffoldingSeedLength(conf);
                break;
            case UNROLL_TANDEM:
                pregelixJobs.add(UnrollTandemRepeat.getConfiguredJob(conf, UnrollTandemRepeat.class));
                break;
            case TIP_SINGLE_NODE:
                pregelixJobs.add(SingleNodeTipRemoveVertex.getConfiguredJob(conf, SingleNodeTipRemoveVertex.class));
                break;
            case TIP:
                pregelixJobs.add(TipRemoveWithSearchVertex.getConfiguredJob(conf, TipRemoveWithSearchVertex.class));
                break; 
            case BUBBLE:
//                pregelixJobs.add(SimpleBubbleMergeVertex.getConfiguredJob(conf, SimpleBubbleMergeVertex.class));
            	conf.set(GenomixJobConf.SCAFFOLDING_INITIAL_DIRECTION, DIR.FORWARD.toString());
            	pregelixJobs.add(BubbleSearchVertex.getConfiguredJob(conf, BubbleSearchVertex.class));
            	conf.set(GenomixJobConf.SCAFFOLDING_INITIAL_DIRECTION, DIR.REVERSE.toString());
            	pregelixJobs.add(BubbleSearchVertex.getConfiguredJob(conf, BubbleSearchVertex.class));
                break;
            /**    
            case SHIFT_LOW_COVERAGE:
                pregelixJobs.add(ShiftLowCoverageReadSetVertex.getConfiguredJob(conf,
                        ShiftLowCoverageReadSetVertex.class));
                break;
                **/
            case REMOVE_BAD_COVERAGE:
            	if(!aggressivePrune){
            		pregelixJobs.add(RemoveBadCoverageVertex.getConfiguredJob(conf, RemoveBadCoverageVertex.class)); 
            		if(Boolean.parseBoolean(conf.get(GenomixJobConf.SCAFFOLDING_CONFIDENT_SEEDS))){
            			aggressivePrune = true;
            			tmpOutput = true;
            		}		
            	}
            	else {
            		conf.set(GenomixJobConf.REMOVE_BAD_COVERAGE_MIN_COVERAGE, conf.
            				get(GenomixJobConf.SCAFFOLDING_CONFIDENT_SEEDS_MIN_COVERAGE));
            		pregelixJobs.add(RemoveBadCoverageVertex.getConfiguredJob(conf, RemoveBadCoverageVertex.class));		
            	}
                break;
            case BRIDGE:
                pregelixJobs.add(BridgeRemoveVertex.getConfiguredJob(conf, BridgeRemoveVertex.class));
                break;
            case RAY_SCAFFOLD:
                throw new IllegalStateException(
                        "RAY_SCAFFOLD should have been expanded to RAY_SCAFFOLD_FORWARD and *_REVERSE!");
            case RAY_SCAFFOLD_FORWARD:
            case RAY_SCAFFOLD_REVERSE:
            	DIR rayDir = (step == Patterns.RAY_SCAFFOLD_FORWARD) ? DIR.FORWARD : DIR.REVERSE;
                conf.set(GenomixJobConf.SCAFFOLDING_INITIAL_DIRECTION, rayDir.toString());

                if (conf.get(GenomixJobConf.SCAFFOLDING_SERIAL_RUN_MIN_LENGTH_THRESHOLD) != null) {
                    // create individual jobs for each Node above threshold, starting at the longest
                    int minLength = Integer.parseInt(conf
                            .get(GenomixJobConf.SCAFFOLDING_SERIAL_RUN_MIN_LENGTH_THRESHOLD));

                    // get all the node lengths
                    flushPendingJobs(conf);
                    TreeMap<Integer, ArrayList<String>> nodeLengths = getNodeLengths(conf, prevOutput);

                    int jobNumber = 0;
                    
                    for (Entry<Integer, ArrayList<String>> lengthEntry : nodeLengths.descendingMap().entrySet()) {
                        if (lengthEntry.getKey() > minLength) {
                            for (String seedId : lengthEntry.getValue()) {
                                //                                for (DIR d : EnumSet.of(DIR.FORWARD, DIR.REVERSE)) {
                                for (DIR d : EnumSet.of(DIR.FORWARD)) {
                                    jobNumber++;
                                    LOG.info("adding job " + jobNumber + " for " + seedId);
                                    curOutput = conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator
                                            + String.format("%02d-", stepNum) + step + "-job-" + jobNumber;
                                    FileInputFormat.setInputPaths(conf, new Path(prevOutput));
                                    FileOutputFormat.setOutputPath(conf, new Path(curOutput));
                                    conf.set(GenomixJobConf.SCAFFOLD_SEED_ID, seedId);
                                    pregelixJobs.add(RayVertex.getConfiguredJob(conf, RayVertex.class));
                                    prevOutput = curOutput;
                                }
                            }
                        }
                    }
                } 
                    else {
                    Float scorePercentile = conf.getFloat(GenomixJobConf.SCAFFOLD_SEED_SCORE_PERCENTILE, -1);
                    Float lengthPercentile = conf.getFloat(GenomixJobConf.SCAFFOLD_SEED_LENGTH_PERCENTILE, -1);
                    if (scorePercentile > 0) {
                        Float topFraction = (scorePercentile > 0 && scorePercentile < 1) ? scorePercentile : null;
                        Integer topCount = (scorePercentile >= 1) ? ((int) scorePercentile.floatValue()) : null;
                        conf.setInt(GenomixJobConf.SCAFFOLDING_SEED_SCORE_THRESHOLD, RayVertex.calculateScoreThreshold(
                                prevStatsCounters, topFraction, topCount, "scaffoldSeedScore-with-" + rayDir));
                    } else {
                        Float topFraction = (lengthPercentile > 0 && lengthPercentile < 1) ? lengthPercentile : null;
                        Integer topCount = (lengthPercentile >= 1) ? ((int) lengthPercentile.floatValue()) : null;
                        conf.setInt(GenomixJobConf.SCAFFOLDING_SEED_LENGTH_THRESHOLD, RayVertex
                                .calculateScoreThreshold(prevStatsCounters, topFraction, topCount, "kmerLength-with-" + rayDir));
                    }
                    conf.setFloat(GenomixJobConf.COVERAGE_NORMAL_MEAN, (float) cur_normalMean);
                    conf.setFloat(GenomixJobConf.COVERAGE_NORMAL_STD, (float) cur_normalStd);
                    pregelixJobs.add(RayVertex.getConfiguredJob(conf, RayVertex.class));
                }
                break;
            case RAY_SCAFFOLD_PRUNE:
            	pregelixJobs.add(PruneVertex.getConfiguredJob(conf, PruneVertex.class));
            	break;
            case LOAD_CONFIDENT_SEEDS:
            	pregelixJobs.add(SeedRetrievalVertex.getConfiguredJob(conf, SeedRetrievalVertex.class));
            	break;
            case SAVE_CONFIDENT_SEEDS:
            	pregelixJobs.add(ConfidentVertex.getConfiguredJob(conf, ConfidentVertex.class));
            	tmpOutput = false;
            	break;
            case DUMP_FASTA:
                flushPendingJobs(conf);
                curOutput = prevOutput + "-DUMP_FASTA";
                if (runLocal) {
                    DriverUtils.dumpGraph(conf, prevOutput, curOutput);
                } else {
                    ConvertToFasta.run(prevOutput, curOutput, threadsPerMachine * numMachines, conf);
                }
                copyToLocalOutputDir(curOutput, conf);
                curOutput = prevOutput; // next job shouldn't use the fasta file
                stepNum--;
                break;
            case CHECK_SYMMETRY:
                pregelixJobs.add(SymmetryCheckerVertex.getConfiguredJob(conf, SymmetryCheckerVertex.class));
                copyToLocalOutputDir(curOutput, conf);
                curOutput = prevOutput; // use previous job's output
                stepNum--;
                break;
            case PLOT_SUBGRAPH:
                plotSubgraph(conf);
                break;
            case STATS:
                prevStatsCounters = runStatsJob(conf);
                break;
            case TIP_ADD:
                pregelixJobs.add(TipAddVertex.getConfiguredJob(conf, TipAddVertex.class));
                break;
            case BRIDGE_ADD:
                pregelixJobs.add(BridgeAddVertex.getConfiguredJob(conf, BridgeAddVertex.class));
                break;
            case BUBBLE_ADD:
                pregelixJobs.add(BubbleAddVertex.getConfiguredJob(conf, BubbleAddVertex.class));
                break;
        }
    }

    /**
     * given an existing HDFS output file, generate a TreeMap of Node lengths
     * 
     * @throws IOException
     */
    private TreeMap<Integer, ArrayList<String>> getNodeLengths(GenomixJobConf conf, String inputGraph)
            throws IOException {
        LOG.info("Getting Map of Node lengths...");
        GenomixJobConf.tick("getNodeLengths");
        TreeMap<Integer, ArrayList<String>> nodeLengths = new TreeMap<>();

        FileSystem dfs = FileSystem.get(conf);
        // stream in the graph, counting elements as you go... this would be better as a hadoop job which aggregated... maybe into counters?
        SequenceFile.Reader reader = null;
        VKmer key = null;
        Node value = null;
        FileStatus[] files = dfs.globStatus(new Path(inputGraph + File.separator + "*"));
        for (FileStatus f : files) {
            if (f.getLen() != 0) {
                try {
                    reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                    key = (VKmer) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    value = (Node) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    while (reader.next(key, value)) {
                        if (key == null || value == null)
                            break;

                        int length = value.getInternalKmer().getKmerLetterLength() == 0 ? key.getKmerLetterLength()
                                : value.getInternalKmer().getKmerLetterLength();
                        if (nodeLengths.containsKey(length)) {
                            nodeLengths.get(length).add(key.toString());
                        } else {
                            nodeLengths.put(length, new ArrayList<>(Collections.singletonList(key.toString())));
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Encountered an error getting lengths for " + f + ":\n" + e);
                } finally {
                    if (reader != null)
                        reader.close();
                }
            }
        }
        LOG.info("Getting length map took " + GenomixJobConf.tock("getNodeLengths") + "ms");
        return nodeLengths;
    }

    private void plotSubgraph(GenomixJobConf conf) throws IOException, Exception {
        if (conf.get(GenomixJobConf.PLOT_SUBGRAPH_START_SEEDS) == "") {
            // no seed specified-- plot the entire graph
            LOG.warning("No starting seed was specified for PLOT_SUBGRAPH.  Plotting the entire graph!!");
            curOutput = prevOutput; // use previous job's output
        } else {
            curOutput = prevOutput + "-SUBGRAPH"; // use previous job's output
            FileOutputFormat.setOutputPath(conf, new Path(curOutput));
            pregelixJobs.add(ExtractSubgraphVertex.getConfiguredJob(conf, ExtractSubgraphVertex.class));
        }
        flushPendingJobs(conf);
        //copy bin to local and append "-PLOT" to the name);
        GenerateGraphViz.writeHDFSBinToHDFSSvg(conf, curOutput, curOutput + "-PLOT",
                GRAPH_TYPE.valueOf(conf.get(GenomixJobConf.PLOT_SUBGRAPH_GRAPH_VERBOSITY)));
        copyToLocalOutputDir(curOutput + "-PLOT", conf);
        curOutput = prevOutput; // next job shouldn't use the truncated graph or plots
        stepNum--;
    }

    private Counters runStatsJob(GenomixJobConf conf) throws Exception, IOException {
        PregelixJob lastJob = null;
        if (pregelixJobs.size() > 0) {
            lastJob = pregelixJobs.get(pregelixJobs.size() - 1);
        }
        flushPendingJobs(conf);
        curOutput = prevOutput + "-STATS";
        Counters counters = GraphStatistics.run(prevOutput, curOutput, conf);
        GraphStatistics.saveGraphStats(curOutput, counters, conf);
        GraphStatistics.drawStatistics(curOutput, counters, conf);
        GraphStatistics.getFastaStatsForGage(curOutput, counters, conf);
        if (lastJob != null) {
            GraphStatistics.saveJobCounters(curOutput, lastJob, conf);
        }
        copyToLocalOutputDir(curOutput, conf);
        curOutput = prevOutput; // use previous job's output
        stepNum--;
        return counters;
    }

    /**
     * Copy a directory from HDFS into the local output directory
     * 
     * @throws IOException
     */
    private void copyToLocalOutputDir(String hdfsSrc, GenomixJobConf conf) throws IOException {
        String localOutputDir = conf.get(GenomixJobConf.LOCAL_OUTPUT_DIR);
        if (localOutputDir != null) {
            if (!FileUtils.fileExists(localOutputDir)) {
                FileUtils.mkdir(localOutputDir);
            }
            FileSystem dfs = FileSystem.get(conf);
            FileSystem.getLocal(conf).delete(new Path(localOutputDir + File.separator + new Path(hdfsSrc).getName()),
                    true);
            dfs.copyToLocalFile(new Path(hdfsSrc), new Path(localOutputDir));
        }
    }

    private void buildGraphWithHyracks(GenomixJobConf conf) throws Exception {
        LOG.info("Building Graph using Hyracks...");
        GenomixJobConf.tick("buildGraphWithHyracks");

        String masterIP = runLocal ? GenomixClusterManager.LOCAL_IP : DriverUtils
                .getIP(conf.get(GenomixJobConf.MASTER));
        int hyracksPort = runLocal ? GenomixClusterManager.LOCAL_HYRACKS_CLIENT_PORT : Integer.parseInt(conf
                .get(GenomixJobConf.HYRACKS_CC_CLIENTPORT));
        hyracksDriver = new GenomixHyracksDriver(masterIP, hyracksPort, threadsPerMachine);
        hyracksDriver.runJob(conf, Plan.BUILD_DEBRUIJN_GRAPH, Boolean.parseBoolean(conf.get(GenomixJobConf.PROFILE)));
        LOG.info("Building the graph took " + GenomixJobConf.tock("buildGraphWithHyracks") + "ms");
    }

    private void buildGraphWithHadoop(GenomixJobConf conf) throws Exception {
        LOG.info("Building Graph using Hadoop...");
        GenomixJobConf.tick("buildGraphWithHadoop");

        GenomixHadoopDriver hadoopDriver = new GenomixHadoopDriver();
        hadoopDriver.run(prevOutput, curOutput, threadsPerMachine * numMachines,
                Integer.parseInt(conf.get(GenomixJobConf.KMER_LENGTH)), 4 * 100000, true, conf);

        LOG.info("Building the graph took " + GenomixJobConf.tock("buildGraphWithHadoop") + "ms");
    }

    /**
     * Run any queued pregelix jobs.
     * Pregelix and non-Pregelix jobs may be interleaved, so we run whatever's waiting.
     */
    private void flushPendingJobs(GenomixJobConf conf) throws Exception {
        if (pregelixJobs.size() > 0) {
            pregelixDriver = new edu.uci.ics.pregelix.core.driver.Driver(this.getClass());
            String masterIP = runLocal ? GenomixClusterManager.LOCAL_IP : DriverUtils.getIP(conf
                    .get(GenomixJobConf.MASTER));
            int pregelixPort = runLocal ? GenomixClusterManager.LOCAL_PREGELIX_CLIENT_PORT : Integer.parseInt(conf
                    .get(GenomixJobConf.PREGELIX_CC_CLIENTPORT));

            // if the user wants to, we can save the intermediate results to HDFS (running each job individually)
            // this would let them resume at arbitrary points of the pipeline
            if (Boolean.parseBoolean(conf.get(GenomixJobConf.SAVE_INTERMEDIATE_RESULTS))) {
                LOG.info("Starting pregelix job series (saving intermediate results)...");
                GenomixJobConf.tick("pregelix-runJob-one-by-one");
                for (int i = 0; i < pregelixJobs.size(); i++) {
                    LOG.info("Starting job " + pregelixJobs.get(i).getJobName());
                    GenomixJobConf.tick("pregelix-job");

                    if (i < pregelixJobs.size() - 1 && 
                    		pregelixJobs.get(i).getJobName().equals(RayVertex.class.getSimpleName()) && 
                    		pregelixJobs.get(i + 1).getJobName().equals(PruneVertex.class.getSimpleName())) {
                    	// prune vertex cannot save to disk :(
                    	pregelixDriver.runJobs(Arrays.asList(pregelixJobs.get(i), pregelixJobs.get(i + 1)), masterIP, pregelixPort);
                    	LOG.info("Finished job " + pregelixJobs.get(i).getJobName() + " in "
                                + GenomixJobConf.tock("pregelix-job") + "ms");
                    	i++;
                    } else {
                    	pregelixDriver.runJob(pregelixJobs.get(i), masterIP, pregelixPort);
                    }

                    LOG.info("Finished job " + pregelixJobs.get(i).getJobName() + " in "
                            + GenomixJobConf.tock("pregelix-job") + "ms");
                }
                LOG.info("Finished job series in " + GenomixJobConf.tock("pregelix-runJob-one-by-one"));
            } else {
                LOG.info("Starting pregelix job series (not saving intermediate results...)");
                GenomixJobConf.tick("pregelix-runJobs");

                pregelixDriver.runJobs(pregelixJobs, masterIP, pregelixPort);

                LOG.info("Finished job series in " + GenomixJobConf.tock("pregelix-runJobs"));
            }
            pregelixJobs.clear();
        }
    }

    private void initGenomix(GenomixJobConf conf) throws Exception {
        GenomixJobConf.setGlobalStaticConstants(conf);
        DriverUtils.loadClusterProperties(conf);
        threadsPerMachine = Integer.parseInt(conf.get(GenomixJobConf.THREADS_PER_MACHINE));
        numMachines = DriverUtils.getSlaveList(conf).length;
        pregelixJobs = new ArrayList<PregelixJob>();
        stepNum = 0;
        aggressivePrune = false;
        runLocal = Boolean.parseBoolean(conf.get(GenomixJobConf.RUN_LOCAL));

        // clear anything in our HDFS work path and local output directory
        FileSystem.get(conf).delete(new Path(conf.get(GenomixJobConf.HDFS_WORK_PATH)), true);
        if (conf.get(GenomixJobConf.LOCAL_OUTPUT_DIR) != null) {
            FileUtils.deleteDirectory(conf.get(GenomixJobConf.LOCAL_OUTPUT_DIR));
        }

        manager = new GenomixClusterManager(runLocal, conf);
        if (!Boolean.parseBoolean(conf.get(GenomixJobConf.USE_EXISTING_CLUSTER))) {
            manager.stopCluster(); // shut down any existing NCs and CCs
            manager.startCluster();
        }
        if (runLocal) {
            manager.renderLocalClusterProperties(); // just create the conf without starting a cluster
        }

        ClusterConfig.setClusterPropertiesPath(System.getProperty("app.home", ".")
                + "/pregelix/conf/cluster.properties");
        ClusterConfig.setStorePath(System.getProperty("app.home", ".") + "/pregelix/conf/stores.properties");
    }

    public void runGenomix(GenomixJobConf conf) throws NumberFormatException, HyracksException, Exception {
        LOG.info("Starting Genomix Assembler Pipeline...");
        GenomixJobConf.tick("runGenomix");

        initGenomix(conf);
        setupHDFSInput(conf);
        // currently, we just iterate over the jobs set in conf[PIPELINE_ORDER].  In the future, we may want more logic to iterate multiple times, etc
        String pipelineSteps = conf.get(GenomixJobConf.PIPELINE_ORDER);
        List<Patterns> allPatterns = new ArrayList<>(Arrays.asList(Patterns.arrayFromString(pipelineSteps)));

        // break up SCAFFOLD into FORWARD and REVERSE steps and insert STATS and MERGE between jobs
        for (int i = 0; i < allPatterns.size(); i++) {
        	if (allPatterns.get(i) == Patterns.REMOVE_BAD_COVERAGE){
        		if (Boolean.parseBoolean(conf.get(GenomixJobConf.SCAFFOLDING_CONFIDENT_SEEDS))){
        			allPatterns.add(i, Patterns.REMOVE_BAD_COVERAGE);
        			allPatterns.add(i, Patterns.REMOVE_BAD_COVERAGE);
        			allPatterns.add(i, Patterns.MERGE);
        			allPatterns.add(i, Patterns.SAVE_CONFIDENT_SEEDS);
        		}
        		
        	}
            if (allPatterns.get(i) == Patterns.RAY_SCAFFOLD) {
                if (i == 0 || allPatterns.get(i - 1) != Patterns.STATS) {
                    allPatterns.set(i, Patterns.STATS);
                    i++;
                } else {
                    allPatterns.remove(i);
                }
                if (Boolean.parseBoolean(conf.get(GenomixJobConf.SCAFFOLDING_CONFIDENT_SEEDS))){
                	allPatterns.add(i, Patterns.LOAD_CONFIDENT_SEEDS);
                	allPatterns.add(i + 1, Patterns.TIP);
                	allPatterns.add(i + 2, Patterns.MERGE);
                	allPatterns.add(i + 3, Patterns.RAY_SCAFFOLD_FORWARD);
                	if (RayVertex.DELAY_PRUNE) {
                    	allPatterns.add(i + 4, Patterns.RAY_SCAFFOLD_PRUNE);
                    }
                } 
                else{
                	allPatterns.add(i, Patterns.RAY_SCAFFOLD_FORWARD);
                	if (RayVertex.DELAY_PRUNE) {
                    	allPatterns.add(i + 1, Patterns.RAY_SCAFFOLD_PRUNE);
                    }
                }
                //allPatterns.add(i + 1, Patterns.MERGE);
                //allPatterns.add(i + 1, Patterns.STATS);
                //allPatterns.add(i, Patterns.RAY_SCAFFOLD_REVERSE);
            }
        }

        if (Boolean.parseBoolean(conf.get(GenomixJobConf.RUN_ALL_STATS))) {
            // insert a STATS step between all jobs that mutate the graph
            for (int i = 0; i < allPatterns.size(); i++) {
                if (((i + 1) == allPatterns.size() || allPatterns.get(i + 1) != Patterns.STATS)
                        && Patterns.mutatingJobs.contains(allPatterns.get(i))) {
                    allPatterns.add(i + 1, Patterns.STATS);
                    i++; // skip the STATS job we just added
                }
            }
        }

        for (Patterns step : allPatterns) {
            stepNum++;
            setOutput(conf, step);
            addStep(conf, step);
        }
        flushPendingJobs(conf);

        if (conf.get(GenomixJobConf.LOCAL_OUTPUT_DIR) != null)
            GenomixClusterManager.copyBinAndTextToLocal(conf, curOutput, conf.get(GenomixJobConf.LOCAL_OUTPUT_DIR)
                    + File.separator + "FINAL-" + new File(curOutput).getName());

        if (conf.get(GenomixJobConf.FINAL_HDFS_OUTPUT_DIR) != null)
            FileSystem.get(conf).rename(new Path(curOutput), new Path(GenomixJobConf.FINAL_HDFS_OUTPUT_DIR));

        LOG.info("Finished the Genomix Assembler Pipeline in " + GenomixJobConf.tock("runGenomix") + "ms!");

        if (!Boolean.parseBoolean(conf.get(GenomixJobConf.USE_EXISTING_CLUSTER))) {
            manager.stopCluster(); // shut down any existing NCs and CCs
        }
    }

    private void setupHDFSInput(GenomixJobConf conf) throws IOException {
        boolean hasLocalInput = false;
        String HDFSInputFromLocalDir = conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator
                + "00-initial-input-from-genomix-driver";

        int libraryId = 0;
        String pairedEndInputs = conf.get(GenomixJobConf.PAIRED_END_FASTQ_INPUTS);
        if (pairedEndInputs != null) {
            String[] inputs = pairedEndInputs.split(",");
            for (int i = 0; i < inputs.length; i += 2) {
                convertAndUploadFastqToHDFS(inputs[i], inputs[i + 1], libraryId, conf, HDFSInputFromLocalDir);
                libraryId++;
                hasLocalInput = true;
            }
        }
        String singleEndInputs = conf.get(GenomixJobConf.SINGLE_END_FASTQ_INPUTS);
        if (singleEndInputs != null) {
            for (String input : singleEndInputs.split(",")) {
                convertAndUploadFastqToHDFS(input, null, libraryId, conf, HDFSInputFromLocalDir);
                libraryId++;
                hasLocalInput = true;
            }
        }
        String localInputDir = conf.get(GenomixJobConf.LOCAL_INPUT_DIR);
        if (localInputDir != null) {
            hasLocalInput = true;
            GenomixClusterManager.copyLocalToHDFS(conf, localInputDir, HDFSInputFromLocalDir);
        }
        if (hasLocalInput) {
            conf.set(GenomixJobConf.INITIAL_HDFS_INPUT_DIR, HDFSInputFromLocalDir);
        }
        curOutput = conf.get(GenomixJobConf.INITIAL_HDFS_INPUT_DIR);
    }

    /**
     * Convert the given fastq file(s) to readid format (mate1\tmate2\n)
     * 
     * @param mate1Fastq
     * @param mate2Fastq
     * @param libraryId
     * @param conf
     * @param outputDir
     * @throws IOException
     */
    private void convertAndUploadFastqToHDFS(String mate1Fastq, String mate2Fastq, int libraryId, GenomixJobConf conf,
            String outputDir) throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        dfs.mkdirs(new Path(outputDir));
        FSDataOutputStream outstream = dfs.create(new Path(outputDir + File.separator + "library-" + libraryId
                + ".readids"), true);
        PrintWriter writer = new PrintWriter(outstream);

        int lineNumber = 0;
        BufferedReader mate1Reader = openFile(mate1Fastq);
        String mate1Line;
        if (mate2Fastq != null) {
            BufferedReader mate2Reader = openFile(mate2Fastq);
            String mate2Line;
            while (true) {
                mate1Line = mate1Reader.readLine();
                mate2Line = mate2Reader.readLine();
                if (mate1Line == null && mate2Line == null) {
                    break;
                } else if (mate1Line == null || mate2Line == null) {
                    throw new IOException("Fastq files " + mate1Fastq + " and " + mate2Fastq
                            + " didn't have the same number of lines! (reached line " + lineNumber
                            + ", last readings: \"" + mate1Line + "\" and " + mate2Line + "\".");
                } else {
                    if ((lineNumber++ % 4) == 1) {
                        writer.print(lineNumber);
                        writer.print('\t');
                        writer.print(mate1Line.trim());
                        writer.print('\t');
                        writer.print(mate2Line.trim());
                        writer.print('\n');
                    }
                }
            }
            mate2Reader.close();
        } else {
            while ((mate1Line = mate1Reader.readLine()) != null) {
                if (lineNumber++ % 4 == 1) {
                    writer.print(lineNumber);
                    writer.print('\t');
                    writer.print(mate1Line.trim());
                    writer.print('\n');
                }
            }
        }

        // Done with the file
        mate1Reader.close();
        writer.close();
    }

    /**
     * return a BufferedReader ready to handle file contents.
     * If the filename ends with ".gz", the file is assumed to be in gzip format.
     * The caller is responsible for closing the returned BufferedReader.
     * 
     * @throws IOException
     */
    private static BufferedReader openFile(String filename) throws IOException {
        FileInputStream fis = new FileInputStream(filename);
        BufferedReader br;
        if (filename.endsWith(".gz")) {
            InputStream gzipStream = new GZIPInputStream(fis); // do I need to explicitly close this reader?  Or will it be closed when the containing BufferedReader is closed?
            Reader decoder = new InputStreamReader(gzipStream, Charset.forName("UTF-8"));
            br = new BufferedReader(decoder);
        } else {
            Reader decoder = new InputStreamReader(fis, Charset.forName("UTF-8"));
            br = new BufferedReader(decoder);
        }
        return br;
    }

    public static void main(String[] args) throws NumberFormatException, HyracksException, Exception {
        GenomixJobConf conf;
        try {
            conf = GenomixJobConf.fromArguments(args);
            String pathToExtraConfFiles = conf.get(GenomixJobConf.EXTRA_CONF_FILES);
            if (pathToExtraConfFiles != "") {
                for (String extraConf : pathToExtraConfFiles.split(",")) {
                    LOG.info("Read job config from " + extraConf);
                    for (Map.Entry<String, String> entry : new JobConf(extraConf)) {
                        conf.setIfUnset(entry.getKey(), entry.getValue());
                    }
                }
            }
        } catch (CmdLineException ex) {
            System.err.println("Usage: bin/genomix [options]\n");
            ex.getParser().setUsageWidth(100);
            ex.getParser().printUsage(System.err);
            System.err.println("\nExample:");
            System.err.println("\tbin/genomix -kmerLength 55 -pipelineOrder BUILD_HYRACKS,MERGE,TIP_REMOVE,MERGE,BUBBLE,MERGE -localInput /path/to/readfiledir/\n");
            System.err.println(ex.getMessage());

            return;
        }
        GenomixDriver driver = new GenomixDriver();
        try {
            driver.runGenomix(conf);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (Boolean.parseBoolean(conf.get(GenomixJobConf.RUN_LOCAL))) {
                // force the in-memory pregelix NC to shut down
                //                System.exit(0);
            }
        }
    }
}
