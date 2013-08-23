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

package edu.uci.ics.genomix.hyracks.graph.driver;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.graph.job.JobGen;
import edu.uci.ics.genomix.hyracks.graph.job.JobGenBrujinGraph;
import edu.uci.ics.genomix.hyracks.graph.job.JobGenCheckReader;
import edu.uci.ics.genomix.hyracks.graph.job.JobGenUnMergedGraph;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

@SuppressWarnings("deprecation")
public class Driver {
    public static enum Plan {
        BUILD_DEBRUJIN_GRAPH,
        CHECK_KMERREADER,
        BUILD_UNMERGED_GRAPH,
    }

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
        runJob(job, Plan.BUILD_UNMERGED_GRAPH, false);
    }

    public void runJob(GenomixJobConf job, Plan planChoice, boolean profiling) throws HyracksException {
        /** add hadoop configurations */
        URL hadoopCore = job.getClass().getClassLoader().getResource("core-site.xml");
        job.addResource(hadoopCore);
        URL hadoopMapRed = job.getClass().getClassLoader().getResource("mapred-site.xml");
        job.addResource(hadoopMapRed);
        URL hadoopHdfs = job.getClass().getClassLoader().getResource("hdfs-site.xml");
        job.addResource(hadoopHdfs);
        
        job.setInt("mapred.line.input.format.linespermap", 2000000); // must be a multiple of 4
        job.setInputFormat(NLineInputFormat.class);

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
                    jobGen = new JobGenBrujinGraph(job, scheduler, ncMap, numPartitionPerMachine);
                    break;
                case CHECK_KMERREADER:
                    jobGen = new JobGenCheckReader(job, scheduler, ncMap, numPartitionPerMachine);
                    break;
                case BUILD_UNMERGED_GRAPH:
                    jobGen = new JobGenUnMergedGraph(job, scheduler, ncMap, numPartitionPerMachine);
                    break;
                default:
                    throw new IllegalArgumentException("Unrecognized planChoice: " + planChoice);
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

    private DeploymentId prepareJobs() throws Exception {
        URLClassLoader classLoader = (URLClassLoader) this.getClass().getClassLoader();
        List<String> jars = new ArrayList<String>();
        URL[] urls = classLoader.getURLs();
        for (URL url : urls)
            if (url.toString().endsWith(".jar"))
                jars.add(new File(url.getPath()).toString());
        DeploymentId deploymentId = hcc.deployBinary(jars);
        return deploymentId;
    }

    private void execute(JobSpecification job) throws Exception {
        job.setUseConnectorPolicyForScheduling(false);
        DeploymentId deployId = prepareJobs();
        JobId jobId = hcc
                .startJob(deployId, job, profiling ? EnumSet.of(JobFlag.PROFILE_RUNTIME) : EnumSet.noneOf(JobFlag.class));
        hcc.waitForCompletion(jobId);
    }
        
    public static void main(String[] args) throws Exception {
//        String[] myArgs = { "-inputDir", "/home/nanz1/TestData", "-outputDir", "/home/hadoop/pairoutput",
//                "-kmerLength", "55", "-ip", "128.195.14.113", "-port", "3099" };
        GenomixJobConf jobConf = GenomixJobConf.fromArguments(args);
        
        String ipAddress = jobConf.get(GenomixJobConf.IP_ADDRESS);
        int port = Integer.parseInt(jobConf.get(GenomixJobConf.PORT));
        int numOfDuplicate = jobConf.getInt(GenomixJobConf.CORES_PER_MACHINE, 4);
        boolean bProfiling = jobConf.getBoolean(GenomixJobConf.PROFILE, true);
        jobConf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_BINARY);
        jobConf.set(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        
        System.out.println(GenomixJobConf.INITIAL_INPUT_DIR);
        System.out.println(GenomixJobConf.FINAL_OUTPUT_DIR);
        FileInputFormat.setInputPaths(jobConf, new Path(jobConf.get(GenomixJobConf.INITIAL_INPUT_DIR)));
        {
          Path path = new Path(jobConf.getWorkingDirectory(), jobConf.get(GenomixJobConf.INITIAL_INPUT_DIR));
          jobConf.set("mapred.input.dir", path.toString());

          Path outputDir = new Path(jobConf.getWorkingDirectory(), jobConf.get(GenomixJobConf.FINAL_OUTPUT_DIR));
          jobConf.set("mapred.output.dir", outputDir.toString());
        }
        
        FileOutputFormat.setOutputPath(jobConf, new Path(jobConf.get(GenomixJobConf.FINAL_OUTPUT_DIR)));
        FileSystem dfs = FileSystem.get(jobConf);
        dfs.delete(new Path(jobConf.get(GenomixJobConf.FINAL_OUTPUT_DIR)), true);
        
        Driver driver = new Driver(ipAddress, port, numOfDuplicate);
        driver.runJob(jobConf, Plan.BUILD_UNMERGED_GRAPH, bProfiling);
    }
}
