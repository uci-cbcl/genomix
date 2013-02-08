package edu.uci.ics.genomix.dataflow;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class PrinterOperatorDescriptor extends
		AbstractSingleActivityOperatorDescriptor {

	private static final long serialVersionUID = 1L;
	private String filename;
	private boolean writeFile;
	private BufferedWriter twriter;
	private FileOutputStream stream;

	/**
	 * The constructor of HDFSWriteOperatorDescriptor.
	 * 
	 * @param spec
	 *            the JobSpecification object
	 * @param conf
	 *            the Hadoop JobConf which contains the output path
	 * @param tupleWriterFactory
	 *            the ITupleWriterFactory implementation object
	 * @throws HyracksException
	 */
	public PrinterOperatorDescriptor(IOperatorDescriptorRegistry spec) {
		super(spec, 1, 0);
		writeFile = false;
	}

	public PrinterOperatorDescriptor(IOperatorDescriptorRegistry spec,
			String filename) {
		super(spec, 1, 0);
		this.filename = filename;
		writeFile = true;
	}

	@Override
	public IOperatorNodePushable createPushRuntime(
			final IHyracksTaskContext ctx,
			final IRecordDescriptorProvider recordDescProvider,
			final int partition, final int nPartitions)
			throws HyracksDataException {

		return new AbstractUnaryInputSinkOperatorNodePushable() {
			private RecordDescriptor inputRd = recordDescProvider
					.getInputRecordDescriptor(getActivityId(), 0);;
			private FrameTupleAccessor accessor = new FrameTupleAccessor(
					ctx.getFrameSize(), inputRd);
			private FrameTupleReference tuple = new FrameTupleReference();

			@Override
			public void open() throws HyracksDataException {
				if (true == writeFile) {
					try {
						filename = filename + String.valueOf(partition)
								+ ".txt";
						// System.err.println(filename);
						stream = new FileOutputStream(filename);
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					twriter = new BufferedWriter(new OutputStreamWriter(stream));
				}
			}

			private void PrintBytes(int no) {
				try {

					byte[] bytes = tuple.getFieldData(no);
					int offset = tuple.getFieldStart(no);
					int length = tuple.getFieldLength(no);
					if (true == writeFile) {
						for (int j = offset; j < offset + length; j++) {
							twriter.write(String.valueOf((int) bytes[j]));
							twriter.write(" ");
						}
						twriter.write("&&");
					} else {
						for (int j = offset; j < offset + length; j++) {
							System.err.print(String.valueOf((int) bytes[j]));
							System.err.print(" ");
						}
						System.err.print("&&");
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void nextFrame(ByteBuffer buffer)
					throws HyracksDataException {
				try {
					accessor.reset(buffer);
					int tupleCount = accessor.getTupleCount();
					for (int i = 0; i < tupleCount; i++) {
						tuple.reset(accessor, i);
						int tj = tuple.getFieldCount();
						for (int j = 0; j < tj; j++) {
							PrintBytes(j);
						}
						if (true == writeFile) {
							twriter.write("\n");
						} else {
							System.err.println();
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void fail() throws HyracksDataException {

			}

			@Override
			public void close() throws HyracksDataException {
				if (true == writeFile) {
					try {
						twriter.close();
						stream.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		};
	}
}
