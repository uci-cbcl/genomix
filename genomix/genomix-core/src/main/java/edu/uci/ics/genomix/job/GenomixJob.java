package edu.uci.ics.genomix.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

public class GenomixJob extends JobConf {

	public static final String JOB_NAME = "genomix";

	/** Kmers length */
	public static final String KMER_LENGTH = "genomix.kmer";
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

	/** Configurations used by hybrid groupby function in graph build phrase */
	public static final String GROUPBY_HYBRID_INPUTSIZE = "genomix.graph.groupby.hybrid.inputsize";
	public static final String GROUPBY_HYBRID_INPUTKEYS = "genomix.graph.groupby.hybrid.inputkeys";
	public static final String GROUPBY_HYBRID_RECORDSIZE_SINGLE = "genomix.graph.groupby.hybrid.recordsize.single";
	public static final String GROUPBY_HYBRID_RECORDSIZE_CROSS = "genomix.graph.groupby.hybrid.recordsize.cross";
	public static final String GROUPBY_HYBRID_HASHLEVEL = "genomix.graph.groupby.hybrid.hashlevel";

	public GenomixJob() throws IOException {
		super(new Configuration());
	}

	public GenomixJob(Configuration conf) throws IOException {
		super(conf);
	}

	/**
	 * Set the kmer length
	 * 
	 * @param the
	 *            desired frame size
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
