package edu.uci.ics.genomix.util;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;

import edu.uci.ics.genomix.data.std.accessors.ByteSerializerDeserializer;
import edu.uci.ics.genomix.type.KmerCountValue;
import edu.uci.ics.genomix.type.KmerUtil;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParserFactory;

public class StatReadsKeyValueParserFactory implements IKeyValueParserFactory<BytesWritable,KmerCountValue> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public IKeyValueParser<BytesWritable,KmerCountValue> createKeyValueParser(IHyracksTaskContext ctx)
			throws HyracksDataException {
		
		final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
		final ByteBuffer outputBuffer = ctx.allocateFrame();
		final FrameTupleAppender outputAppender = new FrameTupleAppender(
				ctx.getFrameSize());
		outputAppender.reset(outputBuffer, true);
		
		return new IKeyValueParser<BytesWritable,KmerCountValue>(){

			@Override
			public void open(IFrameWriter writer) throws HyracksDataException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void parse(BytesWritable key, KmerCountValue value,
					IFrameWriter writer) throws HyracksDataException {
				byte adjMap = value.getAdjBitMap();
				byte count = value.getCount();
				InsertToFrame((byte) (KmerUtil.inDegree(adjMap)*10+KmerUtil.outDegree(adjMap)),count,writer);
			}

			@Override
			public void close(IFrameWriter writer) throws HyracksDataException {
				FrameUtils.flushFrame(outputBuffer, writer);
			}
			
			private void InsertToFrame(byte degree, byte count,
					IFrameWriter writer) {
				try {
					tupleBuilder.reset();
					tupleBuilder.addField(ByteSerializerDeserializer.INSTANCE,degree);
					tupleBuilder.addField(ByteSerializerDeserializer.INSTANCE,count);

					if (!outputAppender.append(
							tupleBuilder.getFieldEndOffsets(),
							tupleBuilder.getByteArray(), 0,
							tupleBuilder.getSize())) {
						FrameUtils.flushFrame(outputBuffer, writer);
						outputAppender.reset(outputBuffer, true);
						if (!outputAppender.append(
								tupleBuilder.getFieldEndOffsets(),
								tupleBuilder.getByteArray(), 0,
								tupleBuilder.getSize())) {
							throw new IllegalStateException(
									"Failed to copy an record into a frame: the record size is too large.");
						}
					}
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		};
	}

}
