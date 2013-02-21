package edu.uci.ics.genomix.dataflow;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;

public class KMerTextWriterFactory implements ITupleWriterFactory {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final int KMER;

	public KMerTextWriterFactory(int kmer) {
		KMER = kmer;
	}

	public class TupleWriter implements ITupleWriter {
		@Override
		public void write(DataOutput output, ITupleReference tuple)
				throws HyracksDataException {
			try {
				Text.writeString(output, Kmer.recoverKmerFrom(KMER,
						tuple.getFieldData(0), tuple.getFieldStart(0),
						tuple.getFieldLength(0)));
				Text.writeString(output, "\t");
				Text.writeString(output, Kmer.recoverAdjacent(tuple
						.getFieldData(1)[tuple.getFieldStart(1)]));
				Text.writeString(output, "\n");
			} catch (IOException e) {
				throw new HyracksDataException(e);
			}
		}
	}

	@Override
	public ITupleWriter getTupleWriter() {
		return new TupleWriter();
	}

}
