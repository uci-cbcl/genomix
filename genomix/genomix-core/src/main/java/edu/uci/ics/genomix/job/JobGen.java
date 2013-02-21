package edu.uci.ics.genomix.job;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public abstract class JobGen {

	protected final Configuration conf;
	protected final GenomixJob genomixJob;
	protected String jobId = new UUID(System.currentTimeMillis(),
			System.nanoTime()).toString();

	public JobGen(GenomixJob job) {
		this.conf = job;
		this.genomixJob = job;
		this.initJobConfiguration();
	}

	protected abstract void initJobConfiguration();

	public abstract JobSpecification generateJob() throws HyracksException;

}