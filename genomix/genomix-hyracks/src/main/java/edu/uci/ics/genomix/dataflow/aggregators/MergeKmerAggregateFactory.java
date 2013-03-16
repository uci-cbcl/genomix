package edu.uci.ics.genomix.dataflow.aggregators;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

/**
 * count
 * 
 */
public class MergeKmerAggregateFactory implements IAggregatorDescriptorFactory {
	private static final long serialVersionUID = 1L;

	public MergeKmerAggregateFactory() {
	}

	@Override
	public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx,
			RecordDescriptor inRecordDescriptor,
			RecordDescriptor outRecordDescriptor, int[] keyFields,
			int[] keyFieldsInPartialResults) throws HyracksDataException {
		return new LocalAggregatorDescriptor();
	}

}
