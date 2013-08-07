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

package edu.uci.ics.genomix.hyracks.newgraph.driver;

import java.net.URL;
import java.util.EnumSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.uci.ics.genomix.hyracks.newgraph.job.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.newgraph.job.JobGen;
import edu.uci.ics.genomix.hyracks.newgraph.job.JobGenBrujinGraph;
import edu.uci.ics.genomix.hyracks.newgraph.job.JobGenCheckReader;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

public class Driver {
    public static enum Plan {
        BUILD_DEBRUJIN_GRAPH,
        CHECK_KMERREADER,
        BUILD_UNMERGED_GRAPH,
    }

    private static final String IS_PROFILING = "genomix.driver.profiling";
    private static final String CPARTITION_PER_MACHINE = "genomix.driver.duplicate.num";
    private static final Log LOG = LogFactory.getLog(Driver.class);
    private JobGen jobGen;
    private boolean profiling;

    private int numPartitionPerMachine;

    private IHyracksClientConnection hcc;
    private Scheduler scheduler;

    public Driver(String ipAddress, int port, int numPartitionPerMachine) throws HyracksException {
        try {
            hcc = new HyracksConnection(ipAddress, port);
            scheduler = new Scheduler(hcc.getNodeControllerInfos());
        } catch (Exception e) {
            throw new HyracksException(e);
        }
        this.numPartitionPerMachine = numPartitionPerMachine;
    }

    public void runJob(GenomixJobConf job) throws HyracksException {
        runJob(job, Plan.BUILD_DEBRUJIN_GRAPH, false);
    }

    public void runJob(GenomixJobConf job, Plan planChoice, boolean profiling) throws HyracksException {
        /** add hadoop configurations */
        URL hadoopCore = job.getClass().getClassLoader().getResource("core-site.xml");
        job.addResource(hadoopCore);
        URL hadoopMapRed = job.getClass().getClassLoader().getResource("mapred-site.xml");
        job.addResource(hadoopMapRed);
        URL hadoopHdfs = job.getClass().getClassLoader().getResource("hdfs-site.xml");
        job.addResource(hadoopHdfs);

        LOG.info("job started");
        long start = System.currentTimeMillis();
        long end = start;
        long time = 0;

        this.profiling = profiling;
        try {
            Map<String, NodeControllerInfo> ncMap = hcc.getNodeControllerInfos();
            LOG.info("ncmap:" + ncMap.size() + " " + ncMap.keySet().toString());
            switch (planChoice) {
                case BUILD_DEBRUJIN_GRAPH:
                default:
                    jobGen = new JobGenBrujinGraph(job, scheduler, ncMap, numPartitionPerMachine);
                    break;
                case CHECK_KMERREADER:
                    jobGen = new JobGenCheckReader(job, scheduler, ncMap, numPartitionPerMachine);
                    break;
                case BUILD_UNMERGED_GRAPH:
                    jobGen = new JobGenUnMergedGraph(job, scheduler, ncMap, numPartitionPerMachine);
            }

            start = System.currentTimeMillis();
            run(jobGen);
            end = System.currentTimeMillis();
            time = end - start;
            LOG.info("result writing finished " + time + "ms");
            LOG.info("job finished");
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    private void run(JobGen jobGen) throws Exception {
        try {
            JobSpecification createJob = jobGen.generateJob();
            execute(createJob);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private void execute(JobSpecification job) throws Exception {
        job.setUseConnectorPolicyForScheduling(false);
        JobId jobId = hcc.startJob(job, profiling ? EnumSet.of(JobFlag.PROFILE_RUNTIME) : EnumSet.noneOf(JobFlag.class));
        hcc.waitForCompletion(jobId);
    }

    public static void main(String[] args) throws Exception {
        GenomixJobConf jobConf = new GenomixJobConf();
        String[] otherArgs = new GenericOptionsParser(jobConf, args).getRemainingArgs();
        if (otherArgs.length < 4) {
            System.err.println("Need <serverIP> <port> <input> <output>");
            System.exit(-1);
        }
        String ipAddress = otherArgs[0];
        int port = Integer.parseInt(otherArgs[1]);
        int numOfDuplicate = jobConf.getInt(CPARTITION_PER_MACHINE, 2);
        boolean bProfiling = jobConf.getBoolean(IS_PROFILING, true);
        // FileInputFormat.setInputPaths(job, otherArgs[2]);
        {
            @SuppressWarnings("deprecation")
            Path path = new Path(jobConf.getWorkingDirectory(), otherArgs[2]);
            jobConf.set("mapred.input.dir", path.toString());

            @SuppressWarnings("deprecation")
            Path outputDir = new Path(jobConf.getWorkingDirectory(), otherArgs[3]);
            jobConf.set("mapred.output.dir", outputDir.toString());
        }
        // FileInputFormat.addInputPath(jobConf, new Path(otherArgs[2]));
        // FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        Driver driver = new Driver(ipAddress, port, numOfDuplicate);
        driver.runJob(jobConf, Plan.BUILD_DEBRUJIN_GRAPH, bProfiling);
    }
}
