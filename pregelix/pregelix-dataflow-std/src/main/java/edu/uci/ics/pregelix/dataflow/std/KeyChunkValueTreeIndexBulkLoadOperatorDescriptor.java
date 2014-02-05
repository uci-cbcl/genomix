package edu.uci.ics.pregelix.dataflow.std;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.file.NoOpLocalResourceFactoryProvider;
import edu.uci.ics.pregelix.dataflow.util.KeyChunkValueUtil;

public class KeyChunkValueTreeIndexBulkLoadOperatorDescriptor extends AbstractTreeIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final float fillFactor;
    private final boolean verifyInput;
    private final long numElementsHint;
    private final boolean checkIfEmptyIndex;
    private final int chunksize;
    private final int[] originKeyFields;

    public KeyChunkValueTreeIndexBulkLoadOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IStorageManagerInterface storageManager, IIndexLifecycleManagerProvider lifecycleManagerProvider,
            IFileSplitProvider fileSplitProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, int[] keyFields, int[] bloomFilterKeyFields,
            float fillFactor, boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex,
            IIndexDataflowHelperFactory dataflowHelperFactory,
            IModificationOperationCallbackFactory modificationOpCallbackFactory, int chunksize) {
        super(spec, 1, 0, null, storageManager, lifecycleManagerProvider, fileSplitProvider, KeyChunkValueUtil
                .convertToKeyChunkValueTypeTraits(typeTraits, keyFields), comparatorFactories, KeyChunkValueUtil
                .convertToKeyChunkValueBloomFilterKeyFields(bloomFilterKeyFields, keyFields), dataflowHelperFactory,
                null, false, NoOpLocalResourceFactoryProvider.INSTANCE, NoOpOperationCallbackFactory.INSTANCE,
                modificationOpCallbackFactory);
        this.fillFactor = fillFactor;
        this.verifyInput = verifyInput;
        this.numElementsHint = numElementsHint;
        this.checkIfEmptyIndex = checkIfEmptyIndex;
        this.originKeyFields = keyFields;
        this.chunksize = chunksize;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new KeyChunkValueTreeIndexBulkLoadOperatorNodePushable(this, ctx, partition, fillFactor, verifyInput,
                numElementsHint, checkIfEmptyIndex, recordDescProvider, originKeyFields, chunksize);
    }
}
