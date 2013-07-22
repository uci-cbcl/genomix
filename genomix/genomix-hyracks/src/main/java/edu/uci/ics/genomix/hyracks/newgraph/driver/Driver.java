package edu.uci.ics.genomix.hyracks.newgraph.driver;

import java.net.URL;
import java.util.EnumSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.uci.ics.genomix.hyracks.driver.Driver.Plan;
import edu.uci.ics.genomix.hyracks.job.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.job.JobGen;
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
        CHECK_KMERREADER,
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
        runJob(job, Plan.CHECK_KMERREADER, false);
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
                case CHECK_KMERREADER:
                default:
                    jobGen = new JobGenCheckReader(job, scheduler, ncMap, numPartitionPerMachine);
                    break;
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
        JobId jobId = hcc
                .startJob(job, profiling ? EnumSet.of(JobFlag.PROFILE_RUNTIME) : EnumSet.noneOf(JobFlag.class));
        hcc.waitForCompletion(jobId);
    }
}
