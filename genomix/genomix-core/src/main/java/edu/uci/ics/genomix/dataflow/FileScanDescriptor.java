package edu.uci.ics.genomix.dataflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.Path;

import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
import edu.uci.ics.genomix.type.Kmer;
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

			private byte[] filter = new byte[4];

			@SuppressWarnings("resource")
			@Override
			public void initialize() {

				tupleBuilder = new ArrayTupleBuilder(2);
				outputBuffer = ctx.allocateFrame();
				outputAppender = new FrameTupleAppender(ctx.getFrameSize());
				outputAppender.reset(outputBuffer, true);

				Kmer.initializeFilter(k, filter);

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
							SplitReads(read.getBytes());
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

			private void SplitReads(byte[] array) {
				try {
					byte[] bytes = null;

					byte pre = 0, next = 0;
					byte r;

					for (int i = 0; i < array.length - k + 1; i++) {
						if (0 == i) {
							bytes = Kmer.CompressKmer(k, array, i);
						} else {
							Kmer.MoveKmer(k, bytes, array[i + k - 1], filter);
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

						/*
						 * System.out.print(l); System.out.print(' ');
						 * System.out.print(r); System.out.println();
						 */

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
