package edu.uci.ics.genomix.driver;

import java.io.IOException;
import java.net.URL;
import java.util.EnumSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.kohsuke.args4j.Option;

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
   
   	private static final String IS_PROFILING = "genomix.driver.profiling";
   	private static final String CPARTITION_PER_MACHINE = "genomix.driver.duplicate.num";
   	private static final String applicationName = GenomixJob.JOB_NAME;
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
    
    public static void main(String [] args) throws Exception{
    	GenomixJob job = new GenomixJob();
    	String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
    	if ( otherArgs.length < 2){
    		System.err.println("Need <serverIP> <port>");
    		System.exit(-1);
    	}
    	String ipAddress = otherArgs[0];
    	int port = Integer.parseInt(otherArgs[1]);
    	int numOfDuplicate = job.getConfiguration().getInt(CPARTITION_PER_MACHINE, 2);
    	boolean bProfiling = job.getConfiguration().getBoolean(IS_PROFILING, true);
    	
    	Driver driver = new Driver(ipAddress, port, numOfDuplicate);
    	driver.runJob(job, Plan.BUILD_DEBRUJIN_GRAPH, bProfiling);
    }
}
