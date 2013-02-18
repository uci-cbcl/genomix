package edu.uci.ics.genomix.dataflow;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.SequenceFile.Writer;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;

public class KMerSequenceWriterFactory implements ITupleWriterFactory {

	private static final long serialVersionUID = 1L;
	private Configuration conf;
	public KMerSequenceWriterFactory(Configuration conf){
		this.conf = conf;
	}
	
	@Override
	public ITupleWriter getTupleWriter() {
		return new ITupleWriter(){

			@Override
			public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
				try {
					Writer writer = SequenceFile.createWriter(conf, (FSDataOutputStream) output, BytesWritable.class, BytesWritable.class, null, null);
					byte[] kmer = tuple.getFieldData(0);
					int keyStart = tuple.getFieldStart(0);
					int keyLength = tuple.getFieldLength(0);
					
					class KMerCountValue implements ValueBytes{
						private ITupleReference tuple;
						public KMerCountValue(ITupleReference tuple) {
							this.tuple = tuple;
						}

						@Override
						public int getSize() {
							return tuple.getFieldLength(1) + tuple.getFieldLength(2);
						}

						@Override
						public void writeCompressedBytes(DataOutputStream arg0)
								throws IllegalArgumentException, IOException {
							for(int i=1; i<3; i++){
								arg0.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
							}
						}

						@Override
						public void writeUncompressedBytes(DataOutputStream arg0)
								throws IOException {
							for(int i=1; i<3; i++){
								arg0.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
							}
						}
						
					}
					for (int i = 0; i < 3; i++) {
						byte[] data = tuple.getFieldData(0);
						int start = tuple.getFieldStart(0);
						int len = tuple.getFieldLength(0);
						output.write(data, start, len);
						output.writeChars(" ");
					}
					writer.appendRaw(kmer, keyStart, keyLength, new KMerCountValue(tuple));
				} catch (IOException e) {
					throw new HyracksDataException(e);
				}
			}
			
		};
	}

}
