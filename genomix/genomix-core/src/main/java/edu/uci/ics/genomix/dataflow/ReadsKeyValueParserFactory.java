package edu.uci.ics.genomix.dataflow;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParserFactory;

public class ReadsKeyValueParserFactory implements
		IKeyValueParserFactory<LongWritable, Text> {
	private static final long serialVersionUID = 1L;

	private int k;
	private int byteNum;
	private byte[] filter = new byte[4];

	public ReadsKeyValueParserFactory(int k) {
		this.k = k;
		byteNum = (byte) Math.ceil((double) k / 4.0);
		filter[0] = (byte) 0xC0;
		filter[1] = (byte) 0xFC;
		filter[2] = 0;

		int r = byteNum * 8 - 2 * k;
		r = 8 - r;
		for (int i = 0; i < r; i++) {
			filter[2] <<= 1;
			filter[2] |= 1;
		}
		for(int i = 0; i < r-1 ; i++){
			filter[3] <<= 1;
		}
	}

	@Override
	public IKeyValueParser<LongWritable, Text> createKeyValueParser(
			final IHyracksTaskContext ctx) {
		;

		final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
		final ByteBuffer outputBuffer = ctx.allocateFrame();
		final FrameTupleAppender outputAppender = new FrameTupleAppender(
				ctx.getFrameSize());
		outputAppender.reset(outputBuffer, true);

		return new IKeyValueParser<LongWritable, Text>() {

			@Override
			public void parse(LongWritable key, Text value, IFrameWriter writer)
					throws HyracksDataException {
				String geneLine = value.toString(); // Read the Real Gene Line
				Pattern genePattern = Pattern.compile("[AGCT]+");
				Matcher geneMatcher = genePattern.matcher(geneLine);
				boolean isValid = geneMatcher.matches();
				if (isValid) {
					SplitReads(geneLine.getBytes(), writer);
				}
			}

			@Override
			public void flush(IFrameWriter writer) throws HyracksDataException {
				FrameUtils.flushFrame(outputBuffer, writer);
			}

			private void SplitReads(byte[] array, IFrameWriter writer) {
				try {
					byte[] bytes = null;

					byte pre = 0, next = 0;
					byte r;

					for (int i = 0; i < array.length - k + 1; i++) {
						if (0 == i) {
							bytes = Kmer.CompressKmer(k,array, i);
						} else {
							Kmer.MoveKmer(k,bytes, array[i + k - 1], filter);
							/*
							 * l <<= 2; l &= window; l |= ConvertSymbol(array[i
							 * + k - 1]);
							 */
							pre = Kmer.GENE_CODE.getAdjBit(array[i - 1]);
						}
						if (i + k != array.length) {
							next = Kmer.GENE_CODE.getAdjBit(array[i + k]);
						}

						r = 0;
						r |= pre;
						r <<= 4;
						r |= next;

						tupleBuilder.reset();

						// tupleBuilder.addField(Integer64SerializerDeserializer.INSTANCE,
						// l);
						tupleBuilder.addField(bytes, 0, byteNum + 1); 
						tupleBuilder.addField(
								ByteSerializerDeserializer.INSTANCE, r);

						// int[] a = tupleBuilder.getFieldEndOffsets();
						// int b = tupleBuilder.getSize();
						// byte[] c = tupleBuilder.getByteArray();

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
					}
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		};
	}

}