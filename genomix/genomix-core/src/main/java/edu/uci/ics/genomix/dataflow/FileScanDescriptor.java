package edu.uci.ics.genomix.dataflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.Path;

import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
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

		byteNum = (byte) Math.ceil((double) k / 4.0);
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
			
			private byte filter0;
			private byte filter1;
			private byte filter2;

			@SuppressWarnings("resource")
			@Override
			public void initialize() {

				tupleBuilder = new ArrayTupleBuilder(2);
				outputBuffer = ctx.allocateFrame();
				outputAppender = new FrameTupleAppender(ctx.getFrameSize());
				outputAppender.reset(outputBuffer, true);
				
				filter0 = (byte) 0xC0;
				filter1 = (byte) 0xFC;
				filter2 = 0;

				int r = byteNum * 8 - 2 * k;
				r = 8 - r;
				for (int i = 0; i < r; i++) {
					filter2 <<= 1;
					filter2 |= 1;
				}
				
				
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
						//int count  = 0;
						while (read != null) {
							read = readsfile.readLine();
							//if(count % 4 == 1)
							SplitReads(read.getBytes());
							// read.getBytes();
							read = readsfile.readLine();
							
							read = readsfile.readLine();

							read = readsfile.readLine();
							//count += 1;
							//System.err.println(count);
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

			private byte[] CompressKmer(byte[] array, int start) {
				// a: 00; c: 01; G: 10; T: 11

				byte[] bytes = new byte[byteNum + 1];
				bytes[0] = (byte) k;

				byte l = 0;
				int count = 0;
				int bcount = 0;

				for (int i = start; i < start+k ; i++) {
					l <<= 2;
					switch (array[i]) {
					case 'A':
					case 'a':
						l |= 0;
						break;
					case 'C':
					case 'c':
						l |= 1;
						break;
					case 'G':
					case 'g':
						l |= 2;
						break;
					case 'T':
					case 't':
						l |= 3;
						break;
					}
					count += 2;
					if (count % 8 == 0 && byteNum != bcount + 1) {
						bcount += 1;
						bytes[byteNum-bcount] = l;
						count = 0;
						l = 0;
					}
				}
				bytes[1] = l;
				return bytes;
			}

			private byte GetBitmap(byte t) {
				byte r = 0;
				switch (t) {
				case 'A':
				case 'a':
					r = 1;
					break;
				case 'C':
				case 'c':
					r = 2;
					break;
				case 'G':
				case 'g':
					r = 4;
					break;
				case 'T':
				case 't':
					r = 8;
					break;
				}
				return r;
			}

			private byte ConvertSymbol(byte t) {
				byte r = 0;
				switch (t) {
				case 'A':
				case 'a':
					r = 0;
					break;
				case 'C':
				case 'c':
					r = 1;
					break;
				case 'G':
				case 'g':
					r = 2;
					break;
				case 'T':
				case 't':
					r = 3;
					break;
				}
				return r;
			}

			void MoveKmer(byte[] bytes, byte c) {
				int i = byteNum;
				bytes[i] <<= 2;
				bytes[i] &= filter2;
				i -= 1;
				while (i > 0) {
					byte f = (byte) (bytes[i] & filter0);
					f >>= 6;
					f &= 3;
					bytes[i + 1] |= f;
					bytes[i] <<= 2;
					bytes[i] &= filter1;
					i -= 1;
				}
				bytes[1] |= ConvertSymbol(c);
			}

			private void SplitReads(byte[] array) {
				try {
					byte[] bytes = null;

					byte pre = 0, next = 0;
					byte r;

					for (int i = 0; i < array.length - k + 1; i++) {
						if (0 == i) {
							bytes = CompressKmer(array, i);
						} else {
							MoveKmer(bytes, array[i + k - 1]);
							/*
							 * l <<= 2; l &= window; l |= ConvertSymbol(array[i
							 * + k - 1]);
							 */
							pre = GetBitmap(array[i - 1]);
						}
						if (i + k != array.length) {
							next = GetBitmap(array[i + k]);
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
