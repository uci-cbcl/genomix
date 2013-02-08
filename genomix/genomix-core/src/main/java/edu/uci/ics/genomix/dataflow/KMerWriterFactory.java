package edu.uci.ics.genomix.dataflow;

import java.io.DataOutput;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;

public class KMerWriterFactory implements ITupleWriterFactory {
	private static final long serialVersionUID = 1L;

	@Override
	public ITupleWriter getTupleWriter() {
		return new ITupleWriter() {
			byte newLine = "\n".getBytes()[0];

			@Override
			public void write(DataOutput output, ITupleReference tuple)
					throws HyracksDataException {
				try {
					for (int i = 0; i < 3; i++) {
						byte[] data = tuple.getFieldData(0);
						int start = tuple.getFieldStart(0);
						int len = tuple.getFieldLength(0);
						output.write(data, start, len);
						output.writeChars(" ");
					}
					output.writeByte(newLine);
				} catch (Exception e) {
					throw new HyracksDataException(e);
				}
			}

		};
	}

}
