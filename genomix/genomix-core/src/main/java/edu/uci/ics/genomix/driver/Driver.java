package edu.uci.ics.genomix.driver;

import java.net.URL;
import java.util.EnumSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.uci.ics.genomix.job.GenomixJob;
import edu.uci.ics.genomix.job.JobGen;
import edu.uci.ics.genomix.job.JobGenBrujinGraph;
import edu.uci.ics.genomix.job.JobGenContigsGeneration;
import edu.uci.ics.genomix.job.JobGenGraphCleanning;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class Driver {
   public static enum Plan {
        BUILD_DEBRUJIN_GRAPH,
        GRAPH_CLEANNING,
        CONTIGS_GENERATION,
    }
   
   	private static final String applicationName = "genomix";
   	private static final Log LOG = LogFactory.getLog(Driver.class);
    private JobGen jobGen;
    private boolean profiling;
    
    private int numPartitionPerMachine;
    
    private IHyracksClientConnection hcc;
    
    public Driver(String ipAddress, int port, int numPartitionPerMachine) throws HyracksException{
    	try{
    		hcc = new HyracksConnection(ipAddress, port);
    	} catch (Exception e) {
            throw new HyracksException(e);
        }
    	this.numPartitionPerMachine = numPartitionPerMachine;
    }
	
    public void runJob(GenomixJob job) throws HyracksException {
        runJob(job, Plan.BUILD_DEBRUJIN_GRAPH, false);
    }

    public void runJob(GenomixJob job, Plan planChoice, boolean profiling)
            throws HyracksException {
        /** add hadoop configurations */
    	//TODO need to include the hadoophome to the classpath in the way below
        URL hadoopCore = job.getClass().getClassLoader().getResource("core-site.xml");
        job.getConfiguration().addResource(hadoopCore);
        URL hadoopMapRed = job.getClass().getClassLoader().getResource("mapred-site.xml");
        job.getConfiguration().addResource(hadoopMapRed);
        URL hadoopHdfs = job.getClass().getClassLoader().getResource("hdfs-site.xml");
        job.getConfiguration().addResource(hadoopHdfs);

        LOG.info("job started");
        long start = System.currentTimeMillis();
        long end = start;
        long time = 0;

        this.profiling = profiling;
        try {
    		Map<String, NodeControllerInfo> ncMap = hcc.getNodeControllerInfos();
            switch (planChoice) {
                case BUILD_DEBRUJIN_GRAPH:default:
                    jobGen = new JobGenBrujinGraph(job, ncMap, numPartitionPerMachine);
                    break;
                case GRAPH_CLEANNING:
                    jobGen = new JobGenGraphCleanning(job);
                    break;
                case CONTIGS_GENERATION:
                    jobGen = new JobGenContigsGeneration(job);
                    break;
            }
            
            start = System.currentTimeMillis();
            runCreate(jobGen);
            end = System.currentTimeMillis();
            time = end - start;
            LOG.info("result writing finished " + time + "ms");
            LOG.info("job finished");
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }
    
    private void runCreate(JobGen jobGen) throws Exception {
        try {
            JobSpecification createJob = jobGen.generateJob();
            execute(createJob);
        } catch (Exception e) {
            throw e;
        }
    }

    private void execute(JobSpecification job) throws Exception {
        job.setUseConnectorPolicyForScheduling(false);
        JobId jobId = hcc.startJob(applicationName, job,
                profiling ? EnumSet.of(JobFlag.PROFILE_RUNTIME) : EnumSet.noneOf(JobFlag.class));
        hcc.waitForCompletion(jobId);
    }
}
