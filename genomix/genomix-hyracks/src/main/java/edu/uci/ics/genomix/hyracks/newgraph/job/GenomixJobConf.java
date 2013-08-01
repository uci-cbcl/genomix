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

package edu.uci.ics.genomix.hyracks.newgraph.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings("deprecation")
public class GenomixJobConf extends JobConf {

    public static final String JOB_NAME = "genomix";

    /** Kmers length */
    public static final String KMER_LENGTH = "genomix.kmerlen";
    /** Read length */
    public static final String READ_LENGTH = "genomix.readlen";
    /** Frame Size */
    public static final String FRAME_SIZE = "genomix.framesize";
    /** Frame Limit, hyracks need */
    public static final String FRAME_LIMIT = "genomix.framelimit";
    /** Table Size, hyracks need */
    public static final String TABLE_SIZE = "genomix.tablesize";
    /** Groupby types */
    public static final String GROUPBY_TYPE = "genomix.graph.groupby.type";
    /** Graph outputformat */
    public static final String OUTPUT_FORMAT = "genomix.graph.output";
    /** Get reversed Kmer Sequence */
    public static final String REVERSED_KMER = "genomix.kmer.reversed";

    /** Configurations used by hybrid groupby function in graph build phrase */
    public static final String GROUPBY_HYBRID_INPUTSIZE = "genomix.graph.groupby.hybrid.inputsize";
    public static final String GROUPBY_HYBRID_INPUTKEYS = "genomix.graph.groupby.hybrid.inputkeys";
    public static final String GROUPBY_HYBRID_RECORDSIZE_SINGLE = "genomix.graph.groupby.hybrid.recordsize.single";
    public static final String GROUPBY_HYBRID_RECORDSIZE_CROSS = "genomix.graph.groupby.hybrid.recordsize.cross";
    public static final String GROUPBY_HYBRID_HASHLEVEL = "genomix.graph.groupby.hybrid.hashlevel";

    public static final int DEFAULT_KMERLEN = 21;
    public static final int DEFAULT_READLEN = 124;
    public static final int DEFAULT_FRAME_SIZE = 128 * 1024;
    public static final int DEFAULT_FRAME_LIMIT = 4096;
    public static final int DEFAULT_TABLE_SIZE = 10485767;
    public static final long DEFAULT_GROUPBY_HYBRID_INPUTSIZE = 154000000L;
    public static final long DEFAULT_GROUPBY_HYBRID_INPUTKEYS = 38500000L;
    public static final int DEFAULT_GROUPBY_HYBRID_RECORDSIZE_SINGLE = 9;
    public static final int DEFAULT_GROUPBY_HYBRID_HASHLEVEL = 1;
    public static final int DEFAULT_GROUPBY_HYBRID_RECORDSIZE_CROSS = 13;

    public static final boolean DEFAULT_REVERSED = true;

    public static final String JOB_PLAN_GRAPHBUILD = "graphbuild";
    public static final String JOB_PLAN_GRAPHSTAT = "graphstat";

    public static final String GROUPBY_TYPE_HYBRID = "hybrid";
    public static final String GROUPBY_TYPE_EXTERNAL = "external";
    public static final String GROUPBY_TYPE_PRECLUSTER = "precluster";
    public static final String OUTPUT_FORMAT_BINARY = "binary";
    public static final String OUTPUT_FORMAT_TEXT = "text";

    public GenomixJobConf() throws IOException {
        super(new Configuration());
    }

    public GenomixJobConf(Configuration conf) throws IOException {
        super(conf);
    }

    /**
     * Set the kmer length
     * 
     * @param the
     *            desired frame kmerByteSize
     */
    final public void setKmerLength(int kmerlength) {
        setInt(KMER_LENGTH, kmerlength);
    }

    final public void setFrameSize(int frameSize) {
        setInt(FRAME_SIZE, frameSize);
    }

    final public void setFrameLimit(int frameLimit) {
        setInt(FRAME_LIMIT, frameLimit);
    }

    final public void setTableSize(int tableSize) {
        setInt(TABLE_SIZE, tableSize);
    }

}
