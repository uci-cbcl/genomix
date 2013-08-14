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

package org.genomix.driver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

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
        @Option(name = "-kmerlength", usage = "The kmer length for this graph.", required = true)
        private int kmerLength = -1;

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
        
        // Hyracks/Pregelix Advanced Setup
        @Option(name = "-ip", usage = "IP address of the cluster controller", required = true)
        private String ipAddress;
        
        @Option(name = "-port", usage = "port of the cluster controller", required = false)
        private int port;
        
        @Option(name = "-profile", usage = "whether to do runtime profifling", required = false)
        private String profile;
    }

    // Global config
    public static final String KMER_LENGTH = "genomix.kmerlength";
    
    // Graph cleaning
    public static final String BRIDGE_REMOVE_MAX_LENGTH = "genomix.bridgeRemove.maxLength";
    public static final String BUBBLE_MERGE_MAX_DISSIMILARITY = "genomix.bubbleMerge.maxDissimilarity";
    public static final String GRAPH_CLEAN_MAX_ITERATIONS = "genomix.graphCleanMaxIterations";
    public static final String PATHMERGE_RANDOM_RANDSEED = "genomix.PathMergeRandom.randSeed";
    public static final String PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD = "genomix.PathMergeRandom.probBeingRandomHead";
    public static final String REMOVE_LOW_COVERAGE_MAX_COVERAGE = "genomix.removeLowCoverage.maxCoverage";
    public static final String TIP_REMOVE_MAX_LENGTH = "genomix.tipRemove.maxLength";
    
    // Hyracks/Pregelix Advanced Setup
    public static final String IP_ADDRESS = "genomix.ipAddress";
    public static final String PORT = "genomix.port";
    public static final String PROFILE = "genomix.profile";

    // TODO should these also be command line options?
    public static final String CPARTITION_PER_MACHINE = "genomix.driver.duplicate.num";
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
    
    public GenomixJobConf() {
        super(new Configuration());
    }

    public GenomixJobConf(Configuration conf) {
        super(conf);
    }
    
    /**
     * Populate a JobConf with default values overridden by command-line options specified in `args`.
     * 
     * Due to the weird way Options are parsed, this requires a throw-away instance of GenomixJobConf
     */
    public void parseArguments(String[] args) throws CmdLineException {
        Options opts = new Options();
        CmdLineParser parser = new CmdLineParser(opts);
        parser.parseArgument(args);
        
        fillDefaultValues(opts);
        validateArgs(opts);
        setFromOptions(opts);
    }
    
    public static GenomixJobConf getDefaultConf() {
        Options opts = new Options();
        GenomixJobConf.fillDefaultValues(opts);
        GenomixJobConf defaults = new GenomixJobConf();
        defaults.setFromOptions(opts);
        return defaults;
    }

    private static void validateArgs(Options opts) throws CmdLineException {
        // Global config
        if (opts.kmerLength < 3)
            throw new CmdLineException("kmerLength must be at least 3!");
        
        // Graph cleaning
        if (opts.bridgeRemove_maxLength < opts.kmerLength)
            throw new CmdLineException("bridgeRemove_maxLength must be at least as long as kmerLength!"); 

        if (opts.bubbleMerge_maxDissimilarity < 0f)
            throw new CmdLineException("bubbleMerge_maxDissimilarity cannot be negative!");
        if (opts.bubbleMerge_maxDissimilarity > 1f)
            throw new CmdLineException("bubbleMerge_maxDissimilarity cannot be greater than 1.0!");
        
        if (opts.graphCleanMaxIterations < 0)
            throw new CmdLineException("graphCleanMaxIterations cannot be negative!");
        
        if (opts.pathMergeRandom_probBeingRandomHead <= 0)
            throw new CmdLineException("pathMergeRandom_probBeingRandomHead greater than 0.0!");
        if (opts.pathMergeRandom_probBeingRandomHead >= 1.0)
            throw new CmdLineException("pathMergeRandom_probBeingRandomHead must be less than 1.0!");
                
        if (opts.removeLowCoverage_maxCoverage < 0)
            throw new CmdLineException("removeLowCoverage_maxCoverage cannot be negative!");
        
        if (opts.tipRemove_maxLength < opts.kmerLength)
            throw new CmdLineException("tipRemove_maxLength must be at least as long as kmerLength!");

        // Hyracks/Pregelix Advanced Setup
        if (opts.ipAddress == null)
            throw new CmdLineException("ipAddress was not specified!");        
    }
    
    private static void fillDefaultValues(Options opts) {
        // Global config
        
        // Graph cleaning
        if (opts.bridgeRemove_maxLength == -1)
            opts.bridgeRemove_maxLength = opts.kmerLength + 1;
        
        if (opts.bubbleMerge_maxDissimilarity == -1)
            opts.bubbleMerge_maxDissimilarity = .05f;
        
        if (opts.graphCleanMaxIterations == -1)
            opts.graphCleanMaxIterations = 10000000;
        
        if (opts.pathMergeRandom_randSeed == -1)
            opts.pathMergeRandom_randSeed = System.currentTimeMillis();
        
        if (opts.pathMergeRandom_probBeingRandomHead == -1)
            opts.pathMergeRandom_probBeingRandomHead = 0.5f;
        
        if (opts.removeLowCoverage_maxCoverage == -1)
            opts.removeLowCoverage_maxCoverage = 1.0f;
        
        if (opts.tipRemove_maxLength == -1)
            opts.tipRemove_maxLength = opts.kmerLength + 1;
    }

    private void setFromOptions(Options opts) {
        // Global config
        setInt(KMER_LENGTH, opts.kmerLength);
                
        // Graph cleaning
        setInt(BRIDGE_REMOVE_MAX_LENGTH, opts.bridgeRemove_maxLength);
        setFloat(BUBBLE_MERGE_MAX_DISSIMILARITY, opts.bubbleMerge_maxDissimilarity);
        setInt(GRAPH_CLEAN_MAX_ITERATIONS, opts.graphCleanMaxIterations);
        setFloat(PATHMERGE_RANDOM_RANDSEED, opts.pathMergeRandom_randSeed);
        setFloat(PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD, opts.pathMergeRandom_probBeingRandomHead);
        setFloat(REMOVE_LOW_COVERAGE_MAX_COVERAGE, opts.removeLowCoverage_maxCoverage);
        setInt(TIP_REMOVE_MAX_LENGTH, opts.tipRemove_maxLength);
        
        // Hyracks/Pregelix Advanced Setup
        set(IP_ADDRESS, opts.ipAddress);
        setInt(PORT, opts.port);
        set(PROFILE, opts.profile);
    }
}
