package edu.uci.ics.genomix.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class GenomixJob extends Job {

	/** Kmers length */
	public static final String KMER_LENGTH = "genomix.kmer";
	/** Frame Size */
	public static final String FRAME_SIZE = "genomix.framesize";
	/** Frame Limit, hyracks need */
	public static final String FRAME_LIMIT = "genomix.framelimit";
	/** Table Size, hyracks need */
	public static final String TABLE_SIZE = "genomix.tablesize";
	/** Groupby types ? */
	public static final String GROUPBY_TYPE = "genomix.graph.groupby.type";
	

	/** Configurations used by hybrid groupby function in graph build phrase */
	public static final String GROUPBY_HYBRID_INPUTSIZE = "genomix.graph.groupby.hybrid.inputsize";
	public static final String GROUPBY_HYBRID_INPUTKEYS = "genomix.graph.groupby.hybrid.inputkeys";
	public static final String GROUPBY_HYBRID_RECORDSIZE_SINGLE = "genomix.graph.groupby.hybrid.recordsize.single";
	public static final String GROUPBY_HYBRID_RECORDSIZE_CROSS = "genomix.graph.groupby.hybrid.recordsize.cross";
	public static final String GROUPBY_HYBRID_HASHLEVEL = "genomix.graph.groupby.hybrid.hashlevel";

	public GenomixJob(String jobname) throws IOException {
		super(new Configuration(), jobname);
	}

	public GenomixJob(Configuration conf, String jobName) throws IOException {
		super(conf, jobName);
	}

	/**
	 * Set the kmer length
	 * 
	 * @param the
	 *            desired frame size
	 */
	final public void setKmerLength(int kmerlength) {
		getConfiguration().setInt(KMER_LENGTH, kmerlength);
	}

	final public void setFrameSize(int frameSize) {
		getConfiguration().setInt(FRAME_SIZE, frameSize);
	}

	final public void setFrameLimit(int frameLimit) {
		getConfiguration().setInt(FRAME_LIMIT, frameLimit);
	}

	final public void setTableSize(int tableSize) {
		getConfiguration().setInt(TABLE_SIZE, tableSize);
	}

}
