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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.plexus.util.FileUtils;
import org.kohsuke.args4j.CmdLineException;

import edu.uci.ics.genomix.data.cluster.DriverUtils;
import edu.uci.ics.genomix.data.cluster.GenomixClusterManager;
import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.config.GenomixJobConf.Patterns;
import edu.uci.ics.genomix.data.utils.GenerateGraphViz;
import edu.uci.ics.genomix.data.utils.GenerateGraphViz.GRAPH_TYPE;
import edu.uci.ics.genomix.hadoop.buildgraph.GenomixHadoopDriver;
import edu.uci.ics.genomix.hadoop.utils.ConvertToFasta;
import edu.uci.ics.genomix.hadoop.utils.GraphStatistics;
import edu.uci.ics.genomix.hyracks.graph.driver.GenomixHyracksDriver;
import edu.uci.ics.genomix.hyracks.graph.driver.GenomixHyracksDriver.Plan;
import edu.uci.ics.genomix.pregelix.operator.bridgeremove.BridgeRemoveVertex;
import edu.uci.ics.genomix.pregelix.operator.extractsubgraph.ExtractSubgraphVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P1ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P4ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.removelowcoverage.RemoveLowCoverageVertex;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.RayVertex;
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
    private String prevOutput;
    private String curOutput;
    private int stepNum;
    private List<PregelixJob> pregelixJobs;
    private boolean runLocal = false;
    private int threadsPerMachine;
    private int numMachines;

    private GenomixClusterManager manager;
    private GenomixHyracksDriver hyracksDriver;
    private edu.uci.ics.pregelix.core.driver.Driver pregelixDriver;

    @SuppressWarnings("deprecation")
    private void setOutput(GenomixJobConf conf, Patterns step) {
        prevOutput = curOutput;
        curOutput = conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator + String.format("%02d-", stepNum) + step;
        FileInputFormat.setInputPaths(conf, new Path(prevOutput));
        FileOutputFormat.setOutputPath(conf, new Path(curOutput));
    }

    private int setMinScaffoldingSeedLength(GenomixJobConf conf) throws IOException{
        Counters counters = GraphStatistics.run(curOutput, curOutput + "-kmerLength-stats", conf);
        long totalNodes = counters.getGroup("totals").getCounter("nodes");
        System.out.println(totalNodes);
        float percentage = 0.2f;
        long numOfSeeds;
        if(Math.abs((float)totalNodes * percentage) < 100)
            numOfSeeds = (long) Math.abs((float)totalNodes * percentage);
        else
            numOfSeeds = 100;
        System.out.println(numOfSeeds);
        
        long curNumOfSeeds = 0;
        for (Counter c : counters.getGroup("kmerLength-bins")){
            curNumOfSeeds += c.getValue();
            if(curNumOfSeeds > numOfSeeds){
                return Integer.parseInt(c.getName());
            }
        }
        throw new IllegalStateException("It is impossible to reach here!");
    }
    
    private void addStep(GenomixJobConf conf, Patterns step) throws Exception {
        // oh, java, why do you pain me so?
        switch (step) {
            case BUILD:
            case BUILD_HYRACKS:
                flushPendingJobs(conf);
                buildGraphWithHyracks(conf);
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
                pregelixJobs.add(SimpleBubbleMergeVertex.getConfiguredJob(conf, SimpleBubbleMergeVertex.class));
                break;
            case LOW_COVERAGE:
                pregelixJobs.add(RemoveLowCoverageVertex.getConfiguredJob(conf, RemoveLowCoverageVertex.class));
                break;
            case BRIDGE:
                pregelixJobs.add(BridgeRemoveVertex.getConfiguredJob(conf, BridgeRemoveVertex.class));
                break;
            case RAY_SCAFFOLD:
                pregelixJobs.add(RayVertex.getConfiguredJob(conf, RayVertex.class));
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
                break;
            case STATS:
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

                    pregelixDriver.runJob(pregelixJobs.get(i), masterIP, pregelixPort);

                    LOG.info("Finished job " + pregelixJobs.get(i).getJobName() + " in "
                            + GenomixJobConf.tock("pregelix-job"));
                }
                LOG.info("Finished job series in " + GenomixJobConf.tock("pregelix-runJob-one-by-one"));
            } else {
                LOG.info("Starting pregelix job series (not saving intermediate results...");
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
        if (Boolean.parseBoolean(conf.get(GenomixJobConf.RUN_ALL_STATS))) {
            // insert a STATS step between all jobs that mutate the graph
            for (int i = 0; i < allPatterns.size(); i++) {
                if (Patterns.mutatingJobs.contains(allPatterns.get(i))) {
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

    /** Convert the given fastq file(s) to readid format (mate1\tmate2\n)
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
            ex.getParser().setUsageWidth(80);
            ex.getParser().printUsage(System.err);
            System.err.println("\nExample:");
            System.err
                    .println("\tbin/genomix -kmerLength 55 -pipelineOrder BUILD_HYRACKS,MERGE,TIP_REMOVE,MERGE,BUBBLE,MERGE -localInput /path/to/readfiledir/\n");
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
                System.exit(0);
            }
        }
    }

}
