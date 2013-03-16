package edu.uci.ics.genomix.dataflow.aggregators;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

/**
 * sum
 * 
 */
public class DistributedMergeLmerAggregateFactory implements
		IAggregatorDescriptorFactory {
	private static final long serialVersionUID = 1L;

	public DistributedMergeLmerAggregateFactory() {
	}

	public class DistributeAggregatorDescriptor extends
			LocalAggregatorDescriptor {
		@Override
		protected byte getCount(IFrameTupleAccessor accessor, int tIndex) {
			return super.getField(accessor, tIndex, 2);
		}
	}

	@Override
	public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx,
			RecordDescriptor inRecordDescriptor,
			RecordDescriptor outRecordDescriptor, int[] keyFields,
			int[] keyFieldsInPartialResults) throws HyracksDataException {
		return new DistributeAggregatorDescriptor();
	}

}
