package edu.uci.ics.pregelix.dataflow.std;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunctionFactory;

public class KeyChunkValueTreeSearchFunctionUpdateOperatorDescriptor extends TreeSearchFunctionUpdateOperatorDescriptor {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private int chunksize;
    private int originFieldsCount;
    private int[] originKeyFieldsMap;

    public KeyChunkValueTreeSearchFunctionUpdateOperatorDescriptor(JobSpecification spec,
            RecordDescriptor dummyRecDesc, IStorageManagerInterface storageManager,
            IIndexLifecycleManagerProvider lcManagerProvider, IFileSplitProvider fileSplitProvider,
            ITypeTraits[] treeIndexTypeTraits, IBinaryComparatorFactory[] comparatorFactories, boolean isForward,
            int chunksize, int originFieldsCount, int[] originKeyFieldsMap, int[] lowKeyFields, int[] highKeyFields,
            boolean lowKeyInclusive, boolean highKeyInclusive, IIndexDataflowHelperFactory dataflowHelperFactory,
            IRecordDescriptorFactory inputRdFactory, int outputArity, IUpdateFunctionFactory functionFactory,
            IRuntimeHookFactory preHookFactory, IRuntimeHookFactory postHookFactory, RecordDescriptor... rDescs) {
        super(spec, dummyRecDesc, storageManager, lcManagerProvider, fileSplitProvider, treeIndexTypeTraits,
                comparatorFactories, isForward, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive,
                dataflowHelperFactory, inputRdFactory, outputArity, functionFactory, preHookFactory, postHookFactory,
                rDescs);
        this.chunksize = chunksize;
        this.originFieldsCount = originFieldsCount;
        this.originKeyFieldsMap = originKeyFieldsMap;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new KeyChunkValueTreeSearchFunctionUpdateOperatorNodePushable(this, ctx, partition, recordDescProvider,
                isForward, chunksize, originFieldsCount, originKeyFieldsMap, lowKeyFields, highKeyFields,
                lowKeyInclusive, highKeyInclusive, functionFactory, preHookFactory, postHookFactory, inputRdFactory,
                outputArity);
    }

}
