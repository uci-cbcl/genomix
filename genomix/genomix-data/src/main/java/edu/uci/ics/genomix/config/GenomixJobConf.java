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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;

@SuppressWarnings("deprecation")
public class GenomixJobConf extends JobConf {

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
        
        @Option(name = "-saveIntermediateResults", usage = "whether or not to save intermediate steps to HDFS (default: false)", required = false)
        private boolean saveIntermediateResults = false;
        

        // Graph cleaning
        @Option(name = "-bridgeRemove_maxLength", usage = "Nodes with length <= bridgeRemoveLength that bridge separate paths are removed from the graph", required = false)
        private int bridgeRemove_maxLength = -1;
        
        @Option(name = "-bubbleMerge_maxDissimilarity", usage = "Maximum dissimilarity (1 - % identity) allowed between two kmers while still considering them a \"bubble\", (leading to their collapse into a single node)", required = false)
        private float bubbleMerge_maxDissimilarity = -1;

        @Option(name = "-graphCleanMaxIterations", usage = "The maximum number of iterations any graph cleaning job is allowed to run for", required = false)
        private int graphCleanMaxIterations = -1;
        
        @Option(name = "-pathMergeRandom_randSeed", usage = "The seed used in the random path-merge algorithm", required = false)
        private long pathMergeRandom_randSeed = -1;
        
        @Option(name = "-pathMergeRandom_probBeingRandomHead", usage = "The probability of being selected as a random head in the random path-merge algorithm", required = false)
        private float pathMergeRandom_probBeingRandomHead = -1;
        
        @Option(name = "-removeLowCoverage_maxCoverage", usage = "Nodes with coverage lower than this threshold will be removed from the graph", required = false)
        private float removeLowCoverage_maxCoverage = -1;
        
        @Option(name = "-tipRemove_maxLength", usage = "Tips (dead ends in the graph) whose length is less than this threshold are removed from the graph", required = false)
        private int tipRemove_maxLength = -1;
        
        // Hyracks/Pregelix Setup
        @Option(name = "-ip", usage = "IP address of the cluster controller", required = false)
        private String ipAddress;
        
        @Option(name = "-port", usage = "Port of the cluster controller", required = false)
        private int port = -1;
        
        @Option(name = "-profile", usage = "Whether or not to do runtime profifling", required = false)
        private boolean profile = false;
        
        @Option(name = "-coresPerMachine", usage="the number of cores available in each machine", required=false)
        private int coresPerMachine;
        
        @Option(name = "-runLocal", usage = "Run a local instance using the Hadoop MiniCluster. NOTE: overrides settings for -ip and -port", required=false)
        private boolean runLocal = false;
        
        @Argument
        private ArrayList<String> arguments = new ArrayList<String>();
    }
    
    /**
     * the set of patterns that can be applied to the graph 
     */
    public static enum Patterns {
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
        LOW_COVERAGE,
        TIP_REMOVE,
        SCAFFOLD,
        SPLIT_REPEAT;
        
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
            return result.toArray(new Patterns[0]);
        }
    }
    
    

    // Global config
    public static final String KMER_LENGTH = "genomix.kmerlength";
    public static final String PIPELINE_ORDER = "genomix.pipelineOrder";
    public static final String INITIAL_INPUT_DIR = "genomix.initial.input.dir";
    public static final String FINAL_OUTPUT_DIR = "genomix.final.output.dir";
    public static final String LOCAL_INPUT_DIR = "genomix.initial.local.input.dir";
    public static final String LOCAL_OUTPUT_DIR = "genomix.final.local.output.dir";
    public static final String SAVE_INTERMEDIATE_RESULTS = "genomix.save.intermediate.results"; 
    
    // Graph cleaning
    public static final String BRIDGE_REMOVE_MAX_LENGTH = "genomix.bridgeRemove.maxLength";
    public static final String BUBBLE_MERGE_MAX_DISSIMILARITY = "genomix.bubbleMerge.maxDissimilarity";
    public static final String GRAPH_CLEAN_MAX_ITERATIONS = "genomix.graphCleanMaxIterations";
    public static final String PATHMERGE_RANDOM_RANDSEED = "genomix.PathMergeRandom.randSeed";
    public static final String PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD = "genomix.PathMergeRandom.probBeingRandomHead";
    public static final String REMOVE_LOW_COVERAGE_MAX_COVERAGE = "genomix.removeLowCoverage.maxCoverage";
    public static final String TIP_REMOVE_MAX_LENGTH = "genomix.tipRemove.maxLength";
    
    // Hyracks/Pregelix Setup
    public static final String IP_ADDRESS = "genomix.ipAddress";
    public static final String PORT = "genomix.port";
    public static final String PROFILE = "genomix.profile";
    public static final String RUN_LOCAL = "genomix.runLocal";

    // TODO should these also be command line options?
    public static final String CORES_PER_MACHINE = "genomix.driver.duplicate.num";
    public static final String FRAME_SIZE = "genomix.framesize";
    public static final String FRAME_LIMIT = "genomix.framelimit";
    public static final String TABLE_SIZE = "genomix.tablesize";
    public static final String GROUPBY_TYPE = "genomix.graph.groupby.type";
    public static final String OUTPUT_FORMAT = "genomix.graph.output";

    /** Configurations used by hybrid groupby function in graph build phrase */
    public static final String GROUPBY_HYBRID_INPUTSIZE = "genomix.graph.groupby.hybrid.inputsize";
    public static final String GROUPBY_HYBRID_INPUTKEYS = "genomix.graph.groupby.hybrid.inputkeys";
    public static final String GROUPBY_HYBRID_RECORDSIZE_SINGLE = "genomix.graph.groupby.hybrid.recordsize.single";
    public static final String GROUPBY_HYBRID_RECORDSIZE_CROSS = "genomix.graph.groupby.hybrid.recordsize.cross";
    public static final String GROUPBY_HYBRID_HASHLEVEL = "genomix.graph.groupby.hybrid.hashlevel";

    public static final int DEFAULT_FRAME_SIZE = 128 * 1024;
    public static final int DEFAULT_FRAME_LIMIT = 4096;
    public static final int DEFAULT_TABLE_SIZE = 10485767;
    public static final long DEFAULT_GROUPBY_HYBRID_INPUTSIZE = 154000000L;
    public static final long DEFAULT_GROUPBY_HYBRID_INPUTKEYS = 38500000L;
    public static final int DEFAULT_GROUPBY_HYBRID_RECORDSIZE_SINGLE = 9;
    public static final int DEFAULT_GROUPBY_HYBRID_HASHLEVEL = 1;
    public static final int DEFAULT_GROUPBY_HYBRID_RECORDSIZE_CROSS = 13;

    public static final String GROUPBY_TYPE_HYBRID = "hybrid";
    public static final String GROUPBY_TYPE_EXTERNAL = "external";
    public static final String GROUPBY_TYPE_PRECLUSTER = "precluster";
    
    public static final String JOB_PLAN_GRAPHBUILD = "graphbuild";
    public static final String JOB_PLAN_GRAPHSTAT = "graphstat";

    public static final String OUTPUT_FORMAT_BINARY = "genomix.outputformat.binary";
    public static final String OUTPUT_FORMAT_TEXT = "genomix.outputformat.text";
    public static final String HDFS_WORK_PATH = "genomix.hdfs.work.path";
    private static final Patterns[] DEFAULT_PIPELINE_ORDER = {
                    Patterns.BUILD, Patterns.MERGE, 
                    Patterns.TIP_REMOVE, Patterns.MERGE,
                    Patterns.BUBBLE, Patterns.MERGE,
                    Patterns.LOW_COVERAGE, Patterns.MERGE,
                    Patterns.SPLIT_REPEAT, Patterns.MERGE,
                    Patterns.SCAFFOLD, Patterns.MERGE
            };
    
    private String[] extraArguments = {};
    
    public GenomixJobConf(int kmerLength) {
        super(new Configuration());
        setInt(KMER_LENGTH, kmerLength);
        fillMissingDefaults();
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
     * 
     * Any command-line options that were unparsed are available via conf.getExtraArguments().
     */
    public static GenomixJobConf fromArguments(String[] args) throws CmdLineException {
        Options opts = new Options();
        CmdLineParser parser = new CmdLineParser(opts);
        parser.parseArgument(args);
        GenomixJobConf conf = new GenomixJobConf(opts.kmerLength);
        conf.extraArguments = opts.arguments.toArray(new String[opts.arguments.size()]);
        conf.setFromOpts(opts);
        conf.fillMissingDefaults();
        validateConf(conf);
        return conf;
    }
    
    /**
     * retrieve any unparsed arguments from parseArguments. Returns an empty array if we weren't initialized use fromArguments()
     */
    public String[] getExtraArguments() {
        return extraArguments;
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

        // Hyracks/Pregelix Advanced Setup
        if (conf.get(IP_ADDRESS) == null)
            throw new IllegalArgumentException("ipAddress was not specified!");        
    }
    
    private void fillMissingDefaults() {
        // Global config
        int kmerLength = getInt(KMER_LENGTH, -1);
        
        // Graph cleaning
        if (getInt(BRIDGE_REMOVE_MAX_LENGTH, -1) == -1 && kmerLength != -1)
            setInt(BRIDGE_REMOVE_MAX_LENGTH, kmerLength + 1);
        
        if (getFloat(BUBBLE_MERGE_MAX_DISSIMILARITY, -1) == -1)
            setFloat(BUBBLE_MERGE_MAX_DISSIMILARITY, .05f);
        
        if (getInt(GRAPH_CLEAN_MAX_ITERATIONS, -1) == -1)
            setInt(GRAPH_CLEAN_MAX_ITERATIONS, 10000000);
        
        if (getFloat(PATHMERGE_RANDOM_RANDSEED, -1) == -1)
            setFloat(PATHMERGE_RANDOM_RANDSEED, System.currentTimeMillis());
        
        if (getFloat(PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD, -1) == -1)
            setFloat(PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD, 0.5f);
        
        if (getFloat(REMOVE_LOW_COVERAGE_MAX_COVERAGE, -1) == -1)
            setFloat(REMOVE_LOW_COVERAGE_MAX_COVERAGE, 1.0f);
        
        if (getInt(TIP_REMOVE_MAX_LENGTH, -1) == -1 && kmerLength != -1)
            setInt(TIP_REMOVE_MAX_LENGTH, kmerLength + 1);
        
        if (get(PIPELINE_ORDER) == null) {
            set(PIPELINE_ORDER, Patterns.stringFromArray(DEFAULT_PIPELINE_ORDER));
        }
        // hdfs setup
        if (get(HDFS_WORK_PATH) == null)
            set(HDFS_WORK_PATH, "genomix_out");  // should be in the user's home directory? 
        
        // hyracks-specific
        if (getInt(CORES_PER_MACHINE, -1) == -1)
            setInt(CORES_PER_MACHINE, 4);
        
        if (getBoolean(RUN_LOCAL, false)) {
            // override any other settings for HOST and PORT
            set(IP_ADDRESS, PregelixHyracksIntegrationUtil.CC_HOST);
            setInt(PORT, PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT);
        }
    }

    private void setFromOpts(Options opts) {
        // Global config
        setInt(KMER_LENGTH, opts.kmerLength);
        if (opts.pipelineOrder != null)
            set(PIPELINE_ORDER, opts.pipelineOrder);
        
        if (opts.localInput != null && opts.hdfsInput != null)
            throw new IllegalArgumentException("Please set either -localInput or -hdfsInput, but NOT BOTH!");
        if (opts.localInput == null && opts.hdfsInput == null)
            throw new IllegalArgumentException("Please specify an input via -localInput or -hdfsInput!");
        if (opts.hdfsInput != null)
            set(INITIAL_INPUT_DIR, opts.hdfsInput);
        if (opts.localInput != null)
            set(LOCAL_INPUT_DIR, opts.localInput);
        if (opts.hdfsOutput != null)
            set(FINAL_OUTPUT_DIR, opts.hdfsOutput);
        if (opts.localOutput != null)
            set(LOCAL_OUTPUT_DIR, opts.localOutput);
        if (opts.hdfsWorkPath != null)
            set(HDFS_WORK_PATH, opts.hdfsWorkPath);
        setBoolean(SAVE_INTERMEDIATE_RESULTS, opts.saveIntermediateResults);
            

        if (opts.runLocal && (opts.ipAddress != null || opts.port != -1))
            throw new IllegalArgumentException("Option -runLocal cannot be set at the same time as -port or -ip! (-runLocal starts a cluster; -ip and -port specify an existing cluster)");
        setBoolean(RUN_LOCAL, opts.runLocal);
        
        // Hyracks/Pregelix Setup
        if (opts.ipAddress != null)
            set(IP_ADDRESS, opts.ipAddress);
        setInt(PORT, opts.port);
        setBoolean(PROFILE, opts.profile);
        setInt(CORES_PER_MACHINE, opts.coresPerMachine);
       
        // Graph cleaning
        setInt(BRIDGE_REMOVE_MAX_LENGTH, opts.bridgeRemove_maxLength);
        setFloat(BUBBLE_MERGE_MAX_DISSIMILARITY, opts.bubbleMerge_maxDissimilarity);
        setInt(GRAPH_CLEAN_MAX_ITERATIONS, opts.graphCleanMaxIterations);
        setFloat(PATHMERGE_RANDOM_RANDSEED, opts.pathMergeRandom_randSeed);
        setFloat(PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD, opts.pathMergeRandom_probBeingRandomHead);
        setFloat(REMOVE_LOW_COVERAGE_MAX_COVERAGE, opts.removeLowCoverage_maxCoverage);
        setInt(TIP_REMOVE_MAX_LENGTH, opts.tipRemove_maxLength);
    }
}
