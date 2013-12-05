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

package edu.uci.ics.genomix.hyracks.graph.job;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

public abstract class JobGen implements Serializable {

    /**
     * generate the jobId
     */
    private static final long serialVersionUID = 1L;

    public static void connectOperators(JobSpecification jobSpec, IOperatorDescriptor preOp, String[] preNodes,
            IOperatorDescriptor nextOp, String[] nextNodes, IConnectorDescriptor conn) {
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, preOp, preNodes);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, nextOp, nextNodes);
        jobSpec.connect(conn, preOp, 0, nextOp, 0);
    }

    protected final ConfFactory hadoopJobConfFactory;
    protected String[] ncNodeNames;

    protected String[] readSchedule;

    protected int frameLimits;
    protected int frameSize;
    protected String jobId = new UUID(System.currentTimeMillis(), System.nanoTime()).toString();

    public JobGen(GenomixJobConf job, Scheduler scheduler, final Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) throws HyracksDataException {
        job.setInputFormat(TextInputFormat.class);
        hadoopJobConfFactory = new ConfFactory(job);
        String[] nodes = new String[ncMap.size()];
        ncMap.keySet().toArray(nodes);
        ncNodeNames = new String[nodes.length * numPartitionPerMachine];
        for (int i = 0; i < numPartitionPerMachine; i++) {
            System.arraycopy(nodes, 0, ncNodeNames, i * nodes.length, nodes.length);
        }
        try {
            InputSplit[] hdfsInputSplits = job.getInputFormat().getSplits(job, ncNodeNames.length);
            readSchedule = scheduler.getLocationConstraints(hdfsInputSplits);
        } catch (IOException ex) {
            throw new HyracksDataException(ex);
        }

        initGenomixConfiguration();
    }

    public InputSplit[] getInputSplit() throws HyracksDataException {
        JobConf jobconf = hadoopJobConfFactory.getConf();
        InputSplit[] hdfsInputSplits;
        try {
            hdfsInputSplits = jobconf.getInputFormat().getSplits(jobconf, ncNodeNames.length);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HyracksDataException("hdfsInputSplits throw exception");
        }
        return hdfsInputSplits;
    }

    protected void initGenomixConfiguration() throws HyracksDataException {
        GenomixJobConf conf = new GenomixJobConf(hadoopJobConfFactory.getConf());
        GenomixJobConf.setGlobalStaticConstants(conf);
        frameLimits = Integer.parseInt(conf.get(GenomixJobConf.FRAME_LIMIT));
        frameSize = Integer.parseInt(conf.get(GenomixJobConf.FRAME_SIZE));
    }

    public JobSpecification generateJob() throws HyracksException {
        JobSpecification job = new JobSpecification();
        job.setFrameSize(frameSize);
        return assignJob(job);
    }

    protected abstract JobSpecification assignJob(JobSpecification job) throws HyracksException;
}
