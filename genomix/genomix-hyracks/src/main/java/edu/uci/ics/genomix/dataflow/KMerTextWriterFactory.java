package edu.uci.ics.genomix.dataflow;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
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
				output.write(Kmer.recoverKmerFrom(KMER,
						tuple.getFieldData(0), tuple.getFieldStart(0),
						tuple.getFieldLength(0)).getBytes());
				output.writeByte('\t');
				output.write(Kmer.GENE_CODE.getSymbolFromBitMap(tuple
						.getFieldData(1)[tuple.getFieldStart(1)]).getBytes());
				output.writeByte('\t');
				output.write(String.valueOf((int)tuple
						.getFieldData(2)[tuple.getFieldStart(2)]).getBytes());
				output.writeByte('\n');
			} catch (IOException e) {
				throw new HyracksDataException(e);
			}
		}

		@Override
		public void open(DataOutput output) throws HyracksDataException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close(DataOutput output) throws HyracksDataException {
			// TODO Auto-generated method stub
		}
	}

	@Override
	public ITupleWriter getTupleWriter(IHyracksTaskContext ctx)
			throws HyracksDataException {
		// TODO Auto-generated method stub
		return new TupleWriter();
	}

}
