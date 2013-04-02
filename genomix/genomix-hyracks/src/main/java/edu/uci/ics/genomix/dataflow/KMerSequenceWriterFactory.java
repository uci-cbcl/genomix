package edu.uci.ics.genomix.dataflow;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.genomix.type.KmerCountValue;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;

@SuppressWarnings("deprecation")
public class KMerSequenceWriterFactory implements ITupleWriterFactory {

	private static final long serialVersionUID = 1L;
	private ConfFactory confFactory;

	public KMerSequenceWriterFactory(JobConf conf) throws HyracksDataException {
		this.confFactory = new ConfFactory(conf);
	}

	public class TupleWriter implements ITupleWriter {
		public TupleWriter(ConfFactory cf) {
			this.cf = cf;
		}

		ConfFactory cf;
		Writer writer = null;

		KmerCountValue reEnterCount = new KmerCountValue();
		BytesWritable reEnterKey = new BytesWritable();

		/**
		 * assumption is that output never change source!
		 */
		@Override
		public void write(DataOutput output, ITupleReference tuple)
				throws HyracksDataException {
			try {
				byte[] kmer = tuple.getFieldData(0);
				int keyStart = tuple.getFieldStart(0);
				int keyLength = tuple.getFieldLength(0);

				byte bitmap = tuple.getFieldData(1)[tuple.getFieldStart(1)];
				byte count = tuple.getFieldData(2)[tuple.getFieldStart(2)];
				reEnterCount.set(bitmap, count);
				reEnterKey.set(kmer, keyStart, keyLength);
				writer.append(reEnterKey, reEnterCount);
				// @mark: this method can not used for read in hadoop 0.20.2.
				// writer.appendRaw(kmer, keyStart, keyLength, reEnterCount);
			} catch (IOException e) {
				throw new HyracksDataException(e);
			}
		}

		@Override
		public void open(DataOutput output) throws HyracksDataException {
			try {
				writer = SequenceFile.createWriter(cf.getConf(),
						(FSDataOutputStream) output, BytesWritable.class,
						KmerCountValue.class, CompressionType.NONE, null);
			} catch (IOException e) {
				throw new HyracksDataException(e);
			}
		}

		@Override
		public void close(DataOutput output) throws HyracksDataException {
			// TODO Auto-generated method stub
		}
	}

	@Override
	public ITupleWriter getTupleWriter(IHyracksTaskContext ctx)
			throws HyracksDataException {
		return new TupleWriter(confFactory);
	}

}
