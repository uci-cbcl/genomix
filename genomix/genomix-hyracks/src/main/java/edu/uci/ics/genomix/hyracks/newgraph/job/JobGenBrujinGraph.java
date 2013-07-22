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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.genomix.hyracks.job.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.job.JobGen;
import edu.uci.ics.genomix.hyracks.newgraph.dataflow.ReadsKeyValueParserFactory;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

@SuppressWarnings("deprecation")
public class JobGenBrujinGraph extends JobGen {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public enum GroupbyType {
        EXTERNAL,
        PRECLUSTER,
        HYBRIDHASH,
    }

    public enum OutputFormat {
        TEXT,
        BINARY,
    }

    protected ConfFactory hadoopJobConfFactory;
    protected static final Log LOG = LogFactory.getLog(JobGenBrujinGraph.class);
    protected String[] ncNodeNames;
    protected String[] readSchedule;

    protected int readLength;
    protected int kmerSize;
    protected int frameLimits;
    protected int frameSize;
    protected int tableSize;
    protected GroupbyType groupbyType;
    protected OutputFormat outputFormat;
    protected boolean bGenerateReversedKmer;

    protected void logDebug(String status) {
        LOG.debug(status + " nc nodes:" + ncNodeNames.length);
    }

    public JobGenBrujinGraph(GenomixJobConf job, Scheduler scheduler, final Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) throws HyracksDataException {
        super(job);
        String[] nodes = new String[ncMap.size()];
        ncMap.keySet().toArray(nodes);
        ncNodeNames = new String[nodes.length * numPartitionPerMachine];
        for (int i = 0; i < numPartitionPerMachine; i++) {
            System.arraycopy(nodes, 0, ncNodeNames, i * nodes.length, nodes.length);
        }
        initJobConfiguration(scheduler);
    }
    
    protected void initJobConfiguration(Scheduler scheduler) throws HyracksDataException {
        Configuration conf = confFactory.getConf();
        readLength = conf.getInt(GenomixJobConf.READ_LENGTH, GenomixJobConf.DEFAULT_READLEN);
        kmerSize = conf.getInt(GenomixJobConf.KMER_LENGTH, GenomixJobConf.DEFAULT_KMERLEN);
        if (kmerSize % 2 == 0) {
            kmerSize--;
            conf.setInt(GenomixJobConf.KMER_LENGTH, kmerSize);
        }
        frameLimits = conf.getInt(GenomixJobConf.FRAME_LIMIT, GenomixJobConf.DEFAULT_FRAME_LIMIT);
        tableSize = conf.getInt(GenomixJobConf.TABLE_SIZE, GenomixJobConf.DEFAULT_TABLE_SIZE);
        frameSize = conf.getInt(GenomixJobConf.FRAME_SIZE, GenomixJobConf.DEFAULT_FRAME_SIZE);

        bGenerateReversedKmer = conf.getBoolean(GenomixJobConf.REVERSED_KMER, GenomixJobConf.DEFAULT_REVERSED);

        String type = conf.get(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        if (type.equalsIgnoreCase(GenomixJobConf.GROUPBY_TYPE_EXTERNAL)) {
            groupbyType = GroupbyType.EXTERNAL;
        } else if (type.equalsIgnoreCase(GenomixJobConf.GROUPBY_TYPE_PRECLUSTER)) {
            groupbyType = GroupbyType.PRECLUSTER;
        } else {
            groupbyType = GroupbyType.HYBRIDHASH;
        }

        String output = conf.get(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_BINARY);
        if (output.equalsIgnoreCase("text")) {
            outputFormat = OutputFormat.TEXT;
        } else {
            outputFormat = OutputFormat.BINARY;
        }
        try {
            hadoopJobConfFactory = new ConfFactory(new JobConf(conf));
            InputSplit[] splits = hadoopJobConfFactory.getConf().getInputFormat()
                    .getSplits(hadoopJobConfFactory.getConf(), ncNodeNames.length);
            readSchedule = scheduler.getLocationConstraints(splits);
        } catch (IOException ex) {
            throw new HyracksDataException(ex);
        }

        LOG.info("Genomix Graph Build Configuration");
        LOG.info("Kmer:" + kmerSize);
        LOG.info("Groupby type:" + type);
        LOG.info("Output format:" + output);
        LOG.info("Frame limit" + frameLimits);
        LOG.info("Frame size" + frameSize);
    }
    
    public HDFSReadOperatorDescriptor createHDFSReader(JobSpecification jobSpec) throws HyracksDataException {
        try {
            InputSplit[] splits = hadoopJobConfFactory.getConf().getInputFormat()
                    .getSplits(hadoopJobConfFactory.getConf(), ncNodeNames.length);

            return new HDFSReadOperatorDescriptor(jobSpec, ReadsKeyValueParserFactory.readKmerOutputRec,
                    hadoopJobConfFactory.getConf(), splits, readSchedule, new ReadsKeyValueParserFactory(readLength,
                            kmerSize, bGenerateReversedKmer));
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public static void connectOperators(JobSpecification jobSpec, IOperatorDescriptor preOp, String[] preNodes,
            IOperatorDescriptor nextOp, String[] nextNodes, IConnectorDescriptor conn) {
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, preOp, preNodes);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, nextOp, nextNodes);
        jobSpec.connect(conn, preOp, 0, nextOp, 0);
    }

    @Override
    public JobSpecification generateJob() throws HyracksException {
        // TODO Auto-generated method stub
        return null;
    }
}
