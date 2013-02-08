package edu.uci.ics.genomix.dataflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import edu.uci.ics.genomix.data.normalizers.Integer64NormalizedKeyComputerFactory;
import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortRunMerger;
import edu.uci.ics.hyracks.dataflow.std.sort.FrameSorter;

public class GenKmerDescriptor extends AbstractOperatorDescriptor {

	private static final long serialVersionUID = 1L;
	private static final int SPLIT_ACTIVITY_ID = 0;
	private static final int MERGE_ACTIVITY_ID = 1;
	private final int framesLimit;
	private final int k;

	public GenKmerDescriptor(IOperatorDescriptorRegistry spec, int framesLimit,
			int k) {
		super(spec, 1, 1);
		this.framesLimit = framesLimit;
		this.k = k;

		// TODO Auto-generated constructor stub
		recordDescriptors[0] = new RecordDescriptor(
				new ISerializerDeserializer[] {
						Integer64SerializerDeserializer.INSTANCE,
						ByteSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE });
	}

	@Override
	public void contributeActivities(IActivityGraphBuilder builder) {
		// TODO Auto-generated method stub
		SplitActivity sa = new SplitActivity(new ActivityId(odId,
				SPLIT_ACTIVITY_ID));
		MergeActivity ma = new MergeActivity(new ActivityId(odId,
				MERGE_ACTIVITY_ID));
		builder.addActivity(this, sa);
		builder.addSourceEdge(0, sa, 0);

		builder.addActivity(this, ma);
		builder.addTargetEdge(0, ma, 0);

		builder.addBlockingEdge(sa, ma);
	}

	private class SplitActivity extends AbstractActivityNode {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public SplitActivity(ActivityId activityID) {
			super(activityID);
			// TODO Auto-generated constructor stub
		}

		public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
				IRecordDescriptorProvider recordDescProvider, int partition,
				int nPartitions) throws HyracksDataException {
			// TODO Auto-generated method stub
			// IHyracksTaskContext ctx, int k, RecordDescriptor rd_in, int
			// buffer_size
			KmerSplitOperatorNodePushable op = new KmerSplitOperatorNodePushable(
					ctx,
					k,
					new RecordDescriptor(
							new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE }),
					framesLimit, new TaskId(this.id, partition));
			return op;
		}
	}

	public static class SplitTaskState extends AbstractStateObject {
		List<IFrameReader> runs;

		public SplitTaskState() {
		}

		public SplitTaskState(JobId jobId, TaskId taskId,
				List<IFrameReader> runs) {
			super(jobId, taskId);
			this.runs = runs;
		}

		@Override
		public void toBytes(DataOutput out) throws IOException {

		}

		@Override
		public void fromBytes(DataInput in) throws IOException {

		}
	}

	private class MergeActivity extends AbstractActivityNode {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public MergeActivity(ActivityId id) {
			super(id);
			// TODO Auto-generated constructor stub
		}

		@Override
		public IOperatorNodePushable createPushRuntime(
				final IHyracksTaskContext ctx,
				IRecordDescriptorProvider recordDescProvider,
				final int partition, int nPartitions)
				throws HyracksDataException {
			// TODO Auto-generated method stub
			IOperatorNodePushable op = new AbstractUnaryOutputSourceOperatorNodePushable() {
				@Override
				public void initialize() throws HyracksDataException {
					SplitTaskState state = (SplitTaskState) ctx
							.getStateObject(new TaskId(new ActivityId(
									getOperatorId(), SPLIT_ACTIVITY_ID),
									partition));
					// List<IFrameReader> runs = runs = new
					// LinkedList<IFrameReader>();;

					IBinaryComparator[] comparators = new IBinaryComparator[1];
					IBinaryComparatorFactory cf = PointableBinaryComparatorFactory
							.of(LongPointable.FACTORY);
					comparators[0] = cf.createBinaryComparator();

					// int necessaryFrames = Math.min(runs.size() + 2,
					// framesLimit);

					FrameSorter frameSorter = new FrameSorter(
							ctx,
							new int[] { 0 },
							new Integer64NormalizedKeyComputerFactory(),
							new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
									.of(LongPointable.FACTORY) },
							recordDescriptors[0]);

					ExternalSortRunMerger merger = new ExternalSortRunMerger(
							ctx, frameSorter, state.runs, new int[] { 0 },
							comparators, recordDescriptors[0], framesLimit,
							writer);
					merger.process();
				}
			};
			return op;
		}
	}
}
