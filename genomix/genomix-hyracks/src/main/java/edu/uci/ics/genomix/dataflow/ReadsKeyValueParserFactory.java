package edu.uci.ics.genomix.dataflow;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.uci.ics.genomix.data.std.accessors.ByteSerializerDeserializer;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.Kmer.GENE_CODE;
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

	public ReadsKeyValueParserFactory(int k) {
		this.k = k;
		byteNum = (byte) Math.ceil((double) k / 4.0);
	}

	@Override
	public IKeyValueParser<LongWritable, Text> createKeyValueParser(
			final IHyracksTaskContext ctx) {
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

			private void SplitReads(byte[] array, IFrameWriter writer) {
				/** first kmer */
				byte[] kmer = Kmer.CompressKmer(k, array, 0);
				byte pre = 0;
				byte next = GENE_CODE.getAdjBit(array[k]);
				InsertToFrame(kmer, pre, next, writer);

				/** middle kmer */
				for (int i = k; i < array.length - 1; i++) {
					pre = Kmer.MoveKmer(k, kmer, array[i]);
					next = GENE_CODE.getAdjBit(array[i + 1]);
					InsertToFrame(kmer, pre, next, writer);

				}
				/** last kmer */
				pre = Kmer.MoveKmer(k, kmer, array[array.length - 1]);
				next = 0;
				InsertToFrame(kmer, pre, next, writer);
			}

			private void InsertToFrame(byte[] kmer, byte pre, byte next,
					IFrameWriter writer) {
				try {
					byte adj = GENE_CODE.mergePreNextAdj(pre, next);
					tupleBuilder.reset();
					tupleBuilder.addField(kmer, 0, byteNum);
					tupleBuilder.addField(ByteSerializerDeserializer.INSTANCE,
							adj);

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

			@Override
			public void open(IFrameWriter writer) throws HyracksDataException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void close(IFrameWriter writer) throws HyracksDataException {
				FrameUtils.flushFrame(outputBuffer, writer);
			}
		};
	}

}