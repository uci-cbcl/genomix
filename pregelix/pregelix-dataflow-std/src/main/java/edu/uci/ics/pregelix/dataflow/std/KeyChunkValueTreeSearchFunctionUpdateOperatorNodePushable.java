package edu.uci.ics.pregelix.dataflow.std;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunctionFactory;
import edu.uci.ics.pregelix.dataflow.util.CopyUpdateUtil;
import edu.uci.ics.pregelix.dataflow.util.KeyChunkValueFrameTupleAppender;
import edu.uci.ics.pregelix.dataflow.util.KeyChunkValueUtil;
import edu.uci.ics.pregelix.dataflow.util.RecoverChunkedTupleBuilder;

public class KeyChunkValueTreeSearchFunctionUpdateOperatorNodePushable extends
        TreeSearchFunctionUpdateOperatorNodePushable {

    private ArrayTupleReference recoveredTuple;
    private PermutingTupleReference permuteTuple;
    private RecoverChunkedTupleBuilder chunkTupleBuilder;
    private ArrayTupleBuilder cachedTreeTupleBuilder;
    private ArrayTupleReference lastTreeTuple;
    private KeyChunkValueFrameTupleAppender chunkAppender;
    private FrameTupleAccessor chunkAccessor;
    private FrameTupleReference chunkTuple = new FrameTupleReference();
    private ArrayTupleBuilder keyBuilder;
    private ArrayTupleBuilder valBuilder;
    private int[] recoveredKeyFields;
    private int chunksize;
    private RecoverChunkedTupleBuilder recoverBuilder;

    public KeyChunkValueTreeSearchFunctionUpdateOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, IRecordDescriptorProvider recordDescProvider, boolean isForward,
            int chunksize, int originFieldsCount, int[] originKeyFieldsMap, int[] lowKeyFields, int[] highKeyFields,
            boolean lowKeyInclusive, boolean highKeyInclusive, IUpdateFunctionFactory functionFactory,
            IRuntimeHookFactory preHookFactory, IRuntimeHookFactory postHookFactory,
            IRecordDescriptorFactory inputRdFactory, int outputArity) throws HyracksDataException {
        super(opDesc, ctx, partition, recordDescProvider, isForward, lowKeyFields, highKeyFields, lowKeyInclusive,
                highKeyInclusive, functionFactory, preHookFactory, postHookFactory, inputRdFactory, outputArity);

        this.recoveredTuple = new ArrayTupleReference();

        this.chunkTupleBuilder = new RecoverChunkedTupleBuilder(originFieldsCount, originKeyFieldsMap, chunksize);
        this.permuteTuple = new PermutingTupleReference(this.chunkTupleBuilder.getPermutation());
        this.cachedTreeTupleBuilder = new ArrayTupleBuilder(originKeyFieldsMap.length + 2);
        this.lastTreeTuple = new ArrayTupleReference();
        this.lastTreeTuple.reset(new int[] {}, new byte[] {});// reset to a empty array to say the last tree tuple is empty
        this.keyBuilder = new ArrayTupleBuilder(originKeyFieldsMap.length);
        this.valBuilder = new ArrayTupleBuilder(1);
        this.recoveredKeyFields = new int[originKeyFieldsMap.length];
        for (int i = 0; i < recoveredKeyFields.length; ++i) {
            this.recoveredKeyFields[i] = i;
        }
        this.chunksize = chunksize;
        this.recoverBuilder = new RecoverChunkedTupleBuilder(originFieldsCount, originKeyFieldsMap, chunksize);
    }

    @Override
    protected void writeSearchResults() throws Exception {
        while (cursor.hasNext()) {
            int chunksBeforeUpdate = KeyChunkValueUtil.recover(chunkTupleBuilder, cachedTreeTupleBuilder,
                    lastTreeTuple, cursor);
            lastTreeTuple.reset(cachedTreeTupleBuilder.getFieldEndOffsets(), cachedTreeTupleBuilder.getByteArray());
            recoveredTuple.reset(chunkTupleBuilder.getFieldEndOffsets(), chunkTupleBuilder.getByteArray());
            permuteTuple.reset(recoveredTuple);
            functionProxy.functionCall(permuteTuple, cloneUpdateTb, cursor);

            KeyChunkValueUtil.reset(chunkAppender, keyBuilder, valBuilder, recoveredTuple, recoveredKeyFields,
                    chunksize);
            chunkAccessor.reset(chunkAppender.getBuffer());

            int chunksAfterUpdate = chunkAccessor.getTupleCount();
            for (int i = 0; i < chunksAfterUpdate; ++i) {
                chunkTuple.reset(chunkAccessor, i);
                // the copyUpdate function will take care of the update and insert (if the update key is non-exist)
                CopyUpdateUtil.copyUpdateChunks(recoverBuilder, tempTupleReference, permuteTuple, updateBuffer,
                        cloneUpdateTb, indexAccessor, cursor, rangePred, true, storageType);
            }

            if (chunksBeforeUpdate > chunksAfterUpdate) {
                //TODO 
                // write the tailored chunks into remove channel.
            }

        }
    }
}
