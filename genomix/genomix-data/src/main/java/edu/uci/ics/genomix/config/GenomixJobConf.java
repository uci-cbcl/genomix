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

package edu.uci.ics.genomix.config;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.genomix.minicluster.GenerateGraphViz.GRAPH_TYPE;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.VKmer;

@SuppressWarnings("deprecation")
public class GenomixJobConf extends JobConf {

    public static boolean debug = false;
    public static ArrayList<VKmer> debugKmers;

    private static Map<String, Long> tickTimes = new HashMap<String, Long>();

    /* The following section ties together command-line options with a global JobConf
     * Each variable has an annotated, command-line Option which is private here but 
     * is accessible through JobConf.get(GenomixConfigOld.VARIABLE).
     * 
     * Default values are set up as part of the .parse() function rather than here since some
     * variables have values defined e.g., with respect to K.
     */

    private static class Options {
        // Global config
        @Option(name = "-kmerLength", usage = "The kmer length for this graph.", required = true)
        private int kmerLength = -1;

        @Option(name = "-num-lines-per-map", usage = "The kmer length for this graph.", required = false)
        private int linesPerMap = -1;

        @Option(name = "-pipelineOrder", usage = "Specify the order of the graph cleaning process", required = false)
        private String pipelineOrder;

        @Option(name = "-localInput", usage = "Local directory containing input for the first pipeline step", required = false)
        private String localInput;

        @Option(name = "-hdfsInput", usage = "HDFS directory containing input for the first pipeline step", required = false)
        private String hdfsInput;

        @Option(name = "-localOutput", usage = "Local directory where the final step's output will be saved", required = false)
        private String localOutput;

        @Option(name = "-hdfsOutput", usage = "HDFS directory where the final step's output will be saved", required = false)
        private String hdfsOutput;

        @Option(name = "-hdfsWorkPath", usage = "HDFS directory where pipeline temp output will be saved", required = false)
        private String hdfsWorkPath;

        @Option(name = "-saveIntermediateResults", usage = "whether or not to save intermediate steps to HDFS (default: true)", required = false)
        private boolean saveIntermediateResults = false;

        @Option(name = "-clusterWaitTime", usage = "the amount of time (in ms) to wait between starting/stopping CC/NC", required = false)
        private int clusterWaitTime = -1;

        // Graph cleaning
        @Option(name = "-bridgeRemove_maxLength", usage = "Nodes with length <= bridgeRemoveLength that bridge separate paths are removed from the graph", required = false)
        private int bridgeRemove_maxLength = -1;

        @Option(name = "-bubbleMerge_maxDissimilarity", usage = "Maximum dissimilarity (1 - % identity) allowed between two kmers while still considering them a \"bubble\", (leading to their collapse into a single node)", required = false)
        private float bubbleMerge_maxDissimilarity = -1;
        
        @Option(name = "-bubbleMergewithsearch_maxLength", usage = "Maximum length can be searched", required = false)
        private int bubbleMergeWithSearch_maxLength = -1;
        
        @Option(name = "-bubbleMergewithsearch_searchDirection", usage = "Maximum length can be searched", required = false)
        private String bubbleMergeWithSearch_searchDirection;
        
        @Option(name = "-graphCleanMaxIterations", usage = "The maximum number of iterations any graph cleaning job is allowed to run for", required = false)
        private int graphCleanMaxIterations = -1;

        @Option(name = "-randomSeed", usage = "The seed used in the random path-merge or split-repeat algorithm", required = false)
        private long randomSeed = -1;

        @Option(name = "-pathMergeRandom_probBeingRandomHead", usage = "The probability of being selected as a random head in the random path-merge algorithm", required = false)
        private float pathMergeRandom_probBeingRandomHead = -1;

        @Option(name = "-removeLowCoverage_maxCoverage", usage = "Nodes with coverage lower than this threshold will be removed from the graph", required = false)
        private float removeLowCoverage_maxCoverage = -1;

        @Option(name = "-tipRemove_maxLength", usage = "Tips (dead ends in the graph) whose length is less than this threshold are removed from the graph", required = false)
        private int tipRemove_maxLength = -1;

        @Option(name = "-maxReadIDsPerEdge", usage = "The maximum number of readids that are recored as spanning a single edge", required = false)
        private int maxReadIDsPerEdge = -1;

        // scaffolding
        @Option(name = "-minScaffoldingTraveralLength", usage = "The minimum length that can be travelled by scaffolding", required = false)
        private int minScaffoldingTraveralLength = -1;

        @Option(name = "-maxScaffoldingTraveralLength", usage = "The maximum length that can be travelled by scaffolding", required = false)
        private int maxScaffoldingTraveralLength = -1;

        @Option(name = "-minScaffoldingVertexMinCoverage", usage = "The minimum vertex coverage that can be the head of scaffolding", required = false)
        private int minScaffoldingVertexMinCoverage = -1;

        @Option(name = "-minScaffoldingVertexMinLength", usage = "The minimum vertex length that can be the head of scaffolding", required = false)
        private int minScaffoldingVertexMinLength = -1;

        @Option(name = "-plotSubgraph_startSeed", usage = "The minimum vertex length that can be the head of scaffolding", required = false)
        private String plotSubgraph_startSeed;

        @Option(name = "-plotSubgraph_numHops", usage = "The minimum vertex length that can be the head of scaffolding", required = false)
        private int plotSubgraph_numHops = -1;

        @Option(name = "-plotSubgraph_verbosity", usage = "Specify the level of details in output graph: 1. UNDIRECTED_GRAPH_WITHOUT_LABELS,"
                + " 2. DIRECTED_GRAPH_WITH_SIMPLELABEL_AND_EDGETYPE, 3. DIRECTED_GRAPH_WITH_KMERS_AND_EDGETYPE, 4. DIRECTED_GRAPH_WITH_ALLDETAILS"
                + "Default is 1.", required = false)
        private int plotSubgraph_verbosity = -1;

        // Hyracks/Pregelix Setup
        @Option(name = "-profile", usage = "Whether or not to do runtime profifling", required = false)
        private boolean profile = false;

        @Option(name = "-runLocal", usage = "Run a local instance using the Hadoop MiniCluster.", required = false)
        private boolean runLocal = false;

        @Option(name = "-useExistingCluster", usage = "Don't start or stop a cluster (use one that's already running)", required = false)
        private boolean useExistingCluster = false;

        @Option(name = "-debugKmers", usage = "Log all interactions with the given comma-separated list of kmers at the FINE log level (check conf/logging.properties to specify an output location)", required = false)
        private String debugKmers = null;

        @Option(name = "-logReadIds", usage = "Log all readIds with the selected edges at the FINE log level (check conf/logging.properties to specify an output location)", required = false)
        private boolean logReadIds = false;

        //Metrics Parameters for Our Mapreduce Gage Job
        @Option(name = "-stats_expectedGenomeSize", usage = "The expected length for this whole genome data", required = false)
        private int stats_expectedGenomeSize = -1;

        @Option(name = "-stats_minContigLength", usage = "the minimum contig length included in statistics calculations", required = false)
        private int stats_minContigLength = -1;

        @Option(name = "-threadsPerMachine", usage = "The number of threads to use per slave machine. Default is 1.", required = false)
        private int threadsPerMachine = 1;

        @Option(name = "-extraConfFiles", usage = "Read all the job confs from the given comma-separated list of multiple conf files", required = false)
        private String extraConfFiles;
    }

    /**
     * the set of patterns that can be applied to the graph
     */
    public enum Patterns {
        BUILD,
        BUILD_HYRACKS,
        BUILD_HADOOP,
        MERGE,
        MERGE_P1,
        MERGE_P2,
        MERGE_P4,
        UNROLL_TANDEM,
        BRIDGE,
        BUBBLE,
        BUBBLE_COMPLEX,
        LOW_COVERAGE,
        TIP_SINGLE_NODE,
        TIP,
        SCAFFOLD,
        RAY_SCAFFOLD,
        SPLIT_REPEAT,
        DUMP_FASTA,
        CHECK_SYMMETRY,
        PLOT_SUBGRAPH,
        STATS,
        TIP_ADD,
        BRIDGE_ADD,
        BUBBLE_ADD,
        BFS;

        /**
         * Get a comma-separated pipeline from the given array of Patterns
         */
        public static String stringFromArray(Patterns[] steps) {
            return StringUtils.join(steps, ",");
        }

        /**
         * Get a Pattern array from a comma-separated list of pipeline steps
         */
        public static Patterns[] arrayFromString(String steps) {
            ArrayList<Patterns> result = new ArrayList<Patterns>();
            for (String p : steps.split(",")) {
                result.add(Patterns.valueOf(p));
            }
            return result.toArray(new Patterns[result.size()]);
        }

        /**
         * make sure a given pattern array is valid
         * BUILD* options must be first
         * only one BUILD* option
         */
        public static void verifyPatterns(Patterns[] patterns) {
            EnumSet<Patterns> buildSteps = EnumSet.of(BUILD, BUILD_HYRACKS, BUILD_HADOOP);
            int lastBuildIndex = -1;
            int countBuildIndex = 0;
            for (int i = patterns.length - 1; i >= 0; i--)
                if (buildSteps.contains(patterns[i])) {
                    lastBuildIndex = i;
                    countBuildIndex++;
                }
            if (countBuildIndex > 1)
                throw new IllegalArgumentException(
                        "Invalid -pipelineOrder specified!  At most one BUILD* step is allowed! Requested -pipelineOrder was "
                                + StringUtils.join(patterns, ","));
            if (lastBuildIndex != -1 && lastBuildIndex != 0)
                throw new IllegalArgumentException(
                        "Invalid -pipelineOrder specified!  BUILD* step must come before all other steps! Requested -pipelineOrder was "
                                + StringUtils.join(patterns, ","));
        }
    }

    // Global config
    public static final String KMER_LENGTH = "genomix.conf.kmerLength";
    public static final String LINES_PERMAP = "genomix.conf.linesPerMap";
    public static final String PIPELINE_ORDER = "genomix.conf.pipelineOrder";
    public static final String INITIAL_HDFS_INPUT_DIR = "genomix.conf.initialHDFSInputDir";
    public static final String FINAL_HDFS_OUTPUT_DIR = "genomix.conf.finalHDFSOutputDir";
    public static final String LOCAL_INPUT_DIR = "genomix.conf.initialLocalInputDir";
    public static final String LOCAL_OUTPUT_DIR = "genomix.conf.finalLocalOutputDir";
    public static final String SAVE_INTERMEDIATE_RESULTS = "genomix.conf.saveIntermediateResults";
    public static final String RANDOM_SEED = "genomix.conf.randomSeed";
    public static final String HDFS_WORK_PATH = "genomix.hdfs.work.path";
    public static final String EXTRA_CONF_FILES = "genomix.conf.extraConfFiles";

    // Graph cleaning   
    public static final String BRIDGE_REMOVE_MAX_LENGTH = "genomix.bridgeRemove.maxLength";
    public static final String BUBBLE_MERGE_MAX_DISSIMILARITY = "genomix.bubbleMerge.maxDissimilarity";
    public static final String BUBBLE_MERGE_WITH_SEARCH_MAX_LENGTH = "genomix.bubbleMergeWithSearch.maxSearchLength";
    public static final String BUBBLE_MERGE_WITH_SEARCH_SEARCH_DIRECTION = "genomix.bubbleMergeWithSearch.searchDirection";
    public static final String GRAPH_CLEAN_MAX_ITERATIONS = "genomix.graphClean.maxIterations";
    public static final String PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD = "genomix.pathMerge.probBeingRandomHead";
    public static final String REMOVE_LOW_COVERAGE_MAX_COVERAGE = "genomix.removeLowCoverage.maxCoverage";
    public static final String TIP_REMOVE_MAX_LENGTH = "genomix.tipRemove.maxLength";
    public static final String MAX_READIDS_PER_EDGE = "genomix.maxReadidsPerEdge";
    public static final String SCAFFOLDING_MIN_TRAVERSAL_LENGTH = "genomix.scaffolding.minTraversalLength";
    public static final String SCAFFOLDING_MAX_TRAVERSAL_LENGTH = "genomix.scaffolding.maxTraversalLength";
    public static final String SCAFFOLDING_VERTEX_MIN_COVERAGE = "genomix.scaffolding.vertexMinCoverage";
    public static final String SCAFFOLDING_VERTEX_MIN_LENGTH = "genomix.scaffolding.vertexMinLength";
    public static final String PLOT_SUBGRAPH_START_SEEDS = "genomix.plotSubgraph.startSeeds";
    public static final String PLOT_SUBGRAPH_NUM_HOPS = "genomix.plotSubgraph.numHops";
    public static final String PLOT_SUBGRAPH_GRAPH_VERBOSITY = "genomix.plotSubgraph.graphVerbosity";

    // Hyracks/Pregelix Setup
    public static final String PROFILE = "genomix.conf.profile";
    public static final String RUN_LOCAL = "genomix.conf.runLocal";
    public static final String USE_EXISTING_CLUSTER = "genomix.conf.useExistingCluster";
    public static final String DEBUG_KMERS = "genomix.conf.debugKmers";
    public static final String LOG_READIDS = "genomix.conf.logReadIds";
    public static final String HYRACKS_GROUPBY_TYPE = "genomix.conf.hyracksGroupby";

    // specified by cluster.properties... hence the different naming convention :(
    public static final String HYRACKS_CC_CLIENTPORT = "HYRACKS_CC_CLIENTPORT";
    public static final String PREGELIX_CC_CLIENTPORT = "PREGELIX_CC_CLIENTPORT";
    public static final String HYRACKS_CC_CLUSTERPORT = "HYRACKS_CC_CLUSTERPORT";
    public static final String PREGELIX_CC_CLUSTERPORT = "PREGELIX_CC_CLUSTERPORT";
    public static final String FRAME_SIZE = "FRAME_SIZE";
    public static final String FRAME_LIMIT = "FRAME_LIMIT";

    public static final String MASTER = "genomix.conf.masterNode";
    public static final String SLAVES = "genomix.conf.slavesList";
    public static final String THREADS_PER_MACHINE = "genomix.threadsPerMachine";

    // GAGE Metrics Evaluation 
    public static final String STATS_EXPECTED_GENOMESIZE = "genomix.conf.expectedGenomeSize";
    public static final String STATS_MIN_CONTIGLENGTH = "genomix.conf.minContigLength";

    // intermediate date evaluation

    public GenomixJobConf(int kmerLength) {
        super(new Configuration());
        setInt(KMER_LENGTH, kmerLength);
        fillMissingDefaults();
        validateConf(this);
    }

    public GenomixJobConf(Configuration other) {
        super(other);
        if (other.get(KMER_LENGTH) == null)
            throw new IllegalArgumentException("Configuration must define KMER_LENGTH!");
        fillMissingDefaults();
        validateConf(this);
    }

    /**
     * Populate a JobConf with default values overridden by command-line options specified in `args`.
     * Any command-line options that were unparsed are available via conf.getExtraArguments().
     */
    public static GenomixJobConf fromArguments(String[] args) throws CmdLineException {

        Options opts = new Options();
        CmdLineParser parser = new CmdLineParser(opts);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            throw e;
        }
        GenomixJobConf conf = new GenomixJobConf(opts.kmerLength);
        conf.setFromOpts(opts);
        conf.fillMissingDefaults();
        validateConf(conf);
        return conf;
    }

    public static void validateConf(GenomixJobConf conf) throws IllegalArgumentException {
        // Global config
        int kmerLength = Integer.parseInt(conf.get(KMER_LENGTH));
        if (kmerLength == -1)
            throw new IllegalArgumentException("kmerLength is unset!");
        if (kmerLength < 3)
            throw new IllegalArgumentException("kmerLength must be at least 3!");

        // Graph cleaning
        if (Integer.parseInt(conf.get(BRIDGE_REMOVE_MAX_LENGTH)) < kmerLength)
            throw new IllegalArgumentException("bridgeRemove_maxLength must be at least as long as kmerLength!");

        if (Float.parseFloat(conf.get(BUBBLE_MERGE_MAX_DISSIMILARITY)) < 0f)
            throw new IllegalArgumentException("bubbleMerge_maxDissimilarity cannot be negative!");
        if (Float.parseFloat(conf.get(BUBBLE_MERGE_MAX_DISSIMILARITY)) > 1f)
            throw new IllegalArgumentException("bubbleMerge_maxDissimilarity cannot be greater than 1.0!");

        if (Integer.parseInt(conf.get(GRAPH_CLEAN_MAX_ITERATIONS)) < 0)
            throw new IllegalArgumentException("graphCleanMaxIterations cannot be negative!");

        if (Float.parseFloat(conf.get(PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD)) <= 0)
            throw new IllegalArgumentException("pathMergeRandom_probBeingRandomHead greater than 0.0!");
        if (Float.parseFloat(conf.get(PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD)) >= 1.0)
            throw new IllegalArgumentException("pathMergeRandom_probBeingRandomHead must be less than 1.0!");

        if (Float.parseFloat(conf.get(REMOVE_LOW_COVERAGE_MAX_COVERAGE)) < 0)
            throw new IllegalArgumentException("removeLowCoverage_maxCoverage cannot be negative!");

        if (Integer.parseInt(conf.get(TIP_REMOVE_MAX_LENGTH)) < kmerLength)
            throw new IllegalArgumentException("tipRemove_maxLength must be at least as long as kmerLength!");

        if (Integer.parseInt(conf.get(MAX_READIDS_PER_EDGE)) < 0)
            throw new IllegalArgumentException("maxReadIDsPerEdge must be non-negative!");

        Patterns.verifyPatterns(Patterns.arrayFromString(conf.get(GenomixJobConf.PIPELINE_ORDER)));
    }

    private void fillMissingDefaults() {
        // Global config
        int kmerLength = getInt(KMER_LENGTH, -1);

        // Graph cleaning
        if (getInt(BRIDGE_REMOVE_MAX_LENGTH, -1) == -1 && kmerLength != -1)
            setInt(BRIDGE_REMOVE_MAX_LENGTH, kmerLength + 1);

        if (getFloat(BUBBLE_MERGE_MAX_DISSIMILARITY, -1) == -1)
            setFloat(BUBBLE_MERGE_MAX_DISSIMILARITY, .5f);

        if (getInt(BUBBLE_MERGE_WITH_SEARCH_MAX_LENGTH, -1) == -1)
            setInt(BUBBLE_MERGE_WITH_SEARCH_MAX_LENGTH, kmerLength * 2);
        
        if (get(BUBBLE_MERGE_WITH_SEARCH_SEARCH_DIRECTION) == null)
            set(BUBBLE_MERGE_WITH_SEARCH_SEARCH_DIRECTION, "FORWARD"); // the default is to search towards FORWARDS
        
        if (getInt(GRAPH_CLEAN_MAX_ITERATIONS, -1) == -1)
            setInt(GRAPH_CLEAN_MAX_ITERATIONS, 10000000);

        if (getLong(RANDOM_SEED, -1) == -1)
            setLong(RANDOM_SEED, System.currentTimeMillis());

        if (getFloat(PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD, -1) == -1)
            setFloat(PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD, 0.5f);

        if (getFloat(REMOVE_LOW_COVERAGE_MAX_COVERAGE, -1) == -1)
            setFloat(REMOVE_LOW_COVERAGE_MAX_COVERAGE, 3.0f);

        if (getInt(TIP_REMOVE_MAX_LENGTH, -1) == -1 && kmerLength != -1)
            setInt(TIP_REMOVE_MAX_LENGTH, kmerLength);

        if (getInt(MAX_READIDS_PER_EDGE, -1) == -1)
            setInt(MAX_READIDS_PER_EDGE, 250);

        // scaffolding
        if (getInt(SCAFFOLDING_MIN_TRAVERSAL_LENGTH, -1) == -1)
            setInt(SCAFFOLDING_MIN_TRAVERSAL_LENGTH, 2);

        if (getInt(SCAFFOLDING_MAX_TRAVERSAL_LENGTH, -1) == -1)
            setInt(SCAFFOLDING_MAX_TRAVERSAL_LENGTH, 15);

        if (getInt(SCAFFOLDING_VERTEX_MIN_COVERAGE, -1) == -1)
            setInt(SCAFFOLDING_VERTEX_MIN_COVERAGE, 1);

        if (getInt(SCAFFOLDING_VERTEX_MIN_LENGTH, -1) == -1)
            setInt(SCAFFOLDING_VERTEX_MIN_LENGTH, 1);

        if (get(PIPELINE_ORDER) == null) {
            set(PIPELINE_ORDER,
                    Patterns.stringFromArray(new Patterns[] { Patterns.BUILD, Patterns.MERGE, Patterns.LOW_COVERAGE,
                            Patterns.MERGE, Patterns.TIP_SINGLE_NODE, Patterns.MERGE, Patterns.BUBBLE, Patterns.MERGE,
                            Patterns.SPLIT_REPEAT, Patterns.MERGE, Patterns.SCAFFOLD, Patterns.MERGE }));
        }

        if (get(PLOT_SUBGRAPH_GRAPH_VERBOSITY) == null)
            set(PLOT_SUBGRAPH_GRAPH_VERBOSITY, GRAPH_TYPE.DIRECTED_GRAPH_WITH_KMERS_AND_EDGETYPE.toString());

        if (get(PLOT_SUBGRAPH_START_SEEDS) == null)
            set(PLOT_SUBGRAPH_START_SEEDS, "");

        if (getInt(PLOT_SUBGRAPH_NUM_HOPS, -1) == -1)
            setInt(PLOT_SUBGRAPH_NUM_HOPS, 1);

        // hdfs setup
        if (get(HDFS_WORK_PATH) == null)
            set(HDFS_WORK_PATH, "genomix_out"); // should be in the user's home directory? 

        // default conf setup
        if (get(EXTRA_CONF_FILES) == null)
            set(EXTRA_CONF_FILES, "");

        // hyracks-specific

        //        if (getBoolean(RUN_LOCAL, false)) {
        //            // override any other settings for HOST and PORT
        //            set(IP_ADDRESS, PregelixHyracksIntegrationUtil.CC_HOST);
        //            setInt(PORT, PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT);
        //        }
    }

    private void setFromOpts(Options opts) {
        // Global config
        setInt(KMER_LENGTH, opts.kmerLength);
        if (opts.pipelineOrder != null)
            set(PIPELINE_ORDER, opts.pipelineOrder);

        if (opts.plotSubgraph_verbosity != -1)
            set(PLOT_SUBGRAPH_GRAPH_VERBOSITY, GRAPH_TYPE.getFromInt(opts.plotSubgraph_verbosity).toString());

        if (opts.localInput != null && opts.hdfsInput != null)
            throw new IllegalArgumentException("Please set either -localInput or -hdfsInput, but NOT BOTH!");
        if (opts.localInput == null && opts.hdfsInput == null)
            throw new IllegalArgumentException("Please specify an input via -localInput or -hdfsInput!");
        if (opts.hdfsInput != null)
            set(INITIAL_HDFS_INPUT_DIR, opts.hdfsInput);
        if (opts.localInput != null)
            set(LOCAL_INPUT_DIR, opts.localInput);
        if (opts.hdfsOutput != null)
            set(FINAL_HDFS_OUTPUT_DIR, opts.hdfsOutput);
        if (opts.localOutput != null)
            set(LOCAL_OUTPUT_DIR, opts.localOutput);
        if (opts.hdfsWorkPath != null)
            set(HDFS_WORK_PATH, opts.hdfsWorkPath);
        setBoolean(SAVE_INTERMEDIATE_RESULTS, opts.saveIntermediateResults);

        setBoolean(RUN_LOCAL, opts.runLocal);

        setBoolean(USE_EXISTING_CLUSTER, opts.useExistingCluster);
        if (opts.debugKmers != null)
            set(DEBUG_KMERS, opts.debugKmers);
        setBoolean(LOG_READIDS, opts.logReadIds);

        // Hyracks/Pregelix Setup
        setBoolean(PROFILE, opts.profile);

        // Graph cleaning
        setInt(BRIDGE_REMOVE_MAX_LENGTH, opts.bridgeRemove_maxLength);
        setFloat(BUBBLE_MERGE_MAX_DISSIMILARITY, opts.bubbleMerge_maxDissimilarity);
        setInt(BUBBLE_MERGE_WITH_SEARCH_MAX_LENGTH, opts.bubbleMergeWithSearch_maxLength);
        if (opts.bubbleMergeWithSearch_searchDirection != null)
            set(BUBBLE_MERGE_WITH_SEARCH_SEARCH_DIRECTION, opts.bubbleMergeWithSearch_searchDirection);
        setInt(GRAPH_CLEAN_MAX_ITERATIONS, opts.graphCleanMaxIterations);
        setLong(RANDOM_SEED, opts.randomSeed);
        setFloat(PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD, opts.pathMergeRandom_probBeingRandomHead);
        setFloat(REMOVE_LOW_COVERAGE_MAX_COVERAGE, opts.removeLowCoverage_maxCoverage);
        setInt(TIP_REMOVE_MAX_LENGTH, opts.tipRemove_maxLength);
        setInt(SCAFFOLDING_MIN_TRAVERSAL_LENGTH, opts.minScaffoldingTraveralLength);
        setInt(SCAFFOLDING_MAX_TRAVERSAL_LENGTH, opts.maxScaffoldingTraveralLength);
        setInt(SCAFFOLDING_VERTEX_MIN_COVERAGE, opts.minScaffoldingVertexMinCoverage);
        setInt(SCAFFOLDING_VERTEX_MIN_LENGTH, opts.minScaffoldingVertexMinLength);

        setInt(STATS_EXPECTED_GENOMESIZE, opts.stats_expectedGenomeSize);
        setInt(STATS_MIN_CONTIGLENGTH, opts.stats_minContigLength);
        setInt(THREADS_PER_MACHINE, opts.threadsPerMachine);
        if (opts.plotSubgraph_startSeed != null)
            set(PLOT_SUBGRAPH_START_SEEDS, opts.plotSubgraph_startSeed);
        setInt(PLOT_SUBGRAPH_NUM_HOPS, opts.plotSubgraph_numHops);

        // read conf.xml
        if (opts.extraConfFiles != null)
            set(EXTRA_CONF_FILES, opts.extraConfFiles);
    }

    /**
     * Reset the given counter, returning the its elapsed time (or 0 if unset)
     */
    public static long tick(String counter) {
        Long time = tickTimes.get(counter);
        tickTimes.put(counter, System.currentTimeMillis());
        if (time == null)
            return 0;
        else
            return System.currentTimeMillis() - time;
    }

    /**
     * Return the given counter without a reset (or 0 if unset)
     */
    public static long tock(String counter) {
        Long time = tickTimes.get(counter);
        if (time == null)
            return 0;
        else
            return System.currentTimeMillis() - time;
    }

    public static void setGlobalStaticConstants(Configuration conf) {
        Kmer.setGlobalKmerLength(Integer.parseInt(conf.get(GenomixJobConf.KMER_LENGTH)));
        //        EdgeWritable.MAX_READ_IDS_PER_EDGE = Integer.parseInt(conf.get(GenomixJobConf.MAX_READIDS_PER_EDGE));
        debug = conf.get(GenomixJobConf.DEBUG_KMERS) != null;
        if (debugKmers == null) {
            debugKmers = new ArrayList<VKmer>();
            if (conf.get(GenomixJobConf.DEBUG_KMERS) != null) {
                for (String kmer : conf.get(GenomixJobConf.DEBUG_KMERS).split(",")) {
                    debugKmers.add(new VKmer(kmer));
                }
            }
        }
    }
}
