package edu.uci.ics.genomix.dataflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.Path;

import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.Kmer.GENE_CODE;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class FileScanDescriptor extends
		AbstractSingleActivityOperatorDescriptor {

	private static final long serialVersionUID = 1L;
	private int k;
	private Path[] filesplit = null;
	private String pathSurfix;
	private int byteNum;

	public FileScanDescriptor(IOperatorDescriptorRegistry spec, int k,
			String path) {
		super(spec, 0, 1);
		this.k = k;
		this.pathSurfix = path;

		byteNum = (int) Math.ceil((double) k / 4.0);
		// recordDescriptors[0] = news RecordDescriptor(
		// new ISerializerDeserializer[] {
		// UTF8StringSerializerDeserializer.INSTANCE });
		recordDescriptors[0] = new RecordDescriptor(
				new ISerializerDeserializer[] { null, null });
	}

	public FileScanDescriptor(JobSpecification jobSpec, int kmers,
			Path[] inputPaths) {
		super(jobSpec, 0, 1);
		this.k = kmers;
		this.filesplit = inputPaths;
		this.pathSurfix = inputPaths[0].toString();
		// recordDescriptors[0] = news RecordDescriptor(
		// new ISerializerDeserializer[] {
		// UTF8StringSerializerDeserializer.INSTANCE });
		recordDescriptors[0] = new RecordDescriptor(
				new ISerializerDeserializer[] { null,
						ByteSerializerDeserializer.INSTANCE });
	}

	public IOperatorNodePushable createPushRuntime(
			final IHyracksTaskContext ctx,
			IRecordDescriptorProvider recordDescProvider, int partition,
			int nPartitions) {

		final int temp = partition;

		// TODO Auto-generated method stub
		return (IOperatorNodePushable) new AbstractUnaryOutputSourceOperatorNodePushable() {
			private ArrayTupleBuilder tupleBuilder;
			private ByteBuffer outputBuffer;
			private FrameTupleAppender outputAppender;

			@SuppressWarnings("resource")
			@Override
			public void initialize() {

				tupleBuilder = new ArrayTupleBuilder(2);
				outputBuffer = ctx.allocateFrame();
				outputAppender = new FrameTupleAppender(ctx.getFrameSize());
				outputAppender.reset(outputBuffer, true);

				try {// one try with multiple catch?
					writer.open();
					String s = pathSurfix + String.valueOf(temp);

					File tf = new File(s);

					File[] fa = tf.listFiles();

					for (int i = 0; i < fa.length; i++) {
						BufferedReader readsfile = new BufferedReader(
								new InputStreamReader(
										new FileInputStream(fa[i])));
						String read = readsfile.readLine();
						// int count = 0;
						while (read != null) {
							read = readsfile.readLine();
							// if(count % 4 == 1)
							SplitReads(read.getBytes(),writer);
							// read.getBytes();
							read = readsfile.readLine();

							read = readsfile.readLine();

							read = readsfile.readLine();
							// count += 1;
							// System.err.println(count);
						}
					}
					if (outputAppender.getTupleCount() > 0) {
						FrameUtils.flushFrame(outputBuffer, writer);
					}
					outputAppender = null;
					outputBuffer = null;
					// sort code for external sort here?
					writer.close();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					throw new IllegalStateException(e);
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
		};

	}
}
