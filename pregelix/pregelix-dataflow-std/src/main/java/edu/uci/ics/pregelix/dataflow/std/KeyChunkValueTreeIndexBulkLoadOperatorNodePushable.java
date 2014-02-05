package edu.uci.ics.pregelix.dataflow.std;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.util.KeyChunkValueFrameTupleAppender;
import edu.uci.ics.pregelix.dataflow.util.KeyChunkValueUtil;

public class KeyChunkValueTreeIndexBulkLoadOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {

    private final IIndexOperatorDescriptor opDesc;
    private final IHyracksTaskContext ctx;
    private final float fillFactor;
    private final boolean verifyInput;
    private final long numElementsHint;
    private final boolean checkIfEmptyIndex;
    private final IIndexDataflowHelper indexHelper;
    private IIndex index;
    private IIndexBulkLoader bulkLoader;
    private IRecordDescriptorProvider recDescProvider;

    private FrameTupleAccessor originAccessor;
    private FrameTupleReference originTuple = new FrameTupleReference();

    private final int[] originKeyFields;
    private KeyChunkValueFrameTupleAppender chunkAppender;
    private ArrayTupleBuilder chunkKeyBuilder;
    private ArrayTupleBuilder chunkValBuilder;
    private FrameTupleAccessor chunkAccessor;
    private FrameTupleReference chunkTuple = new FrameTupleReference();
    private final int chunksize;
    private ByteBuffer appenderBuffer;

    public KeyChunkValueTreeIndexBulkLoadOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, float fillFactor, boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex,
            IRecordDescriptorProvider recordDescProvider, int[] keyFields, int chunksize) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.indexHelper = opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(opDesc, ctx, partition);
        this.fillFactor = fillFactor;
        this.verifyInput = verifyInput;
        this.numElementsHint = numElementsHint;
        this.checkIfEmptyIndex = checkIfEmptyIndex;
        this.recDescProvider = recordDescProvider;
        this.chunksize = chunksize;
        this.chunkAppender = new KeyChunkValueFrameTupleAppender(ctx.getFrameSize());
        this.originKeyFields = keyFields;
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] sd = new ISerializerDeserializer[originKeyFields.length + 2];
        this.chunkAccessor = new FrameTupleAccessor(ctx.getFrameSize(), new RecordDescriptor(sd));
        this.chunkKeyBuilder = new ArrayTupleBuilder(originKeyFields.length);
        this.chunkValBuilder = new ArrayTupleBuilder(1);
    }

    @Override
    public void open() throws HyracksDataException {
        RecordDescriptor recDesc = recDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        originAccessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        try {
            bulkLoader = index.createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex);
            appenderBuffer = ctx.allocateFrame();
            appenderBuffer.position(0);
            appenderBuffer.limit(appenderBuffer.capacity());
        } catch (Exception e) {
            indexHelper.close();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        originAccessor.reset(buffer);
        int tupleCount = originAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            originTuple.reset(originAccessor, i);
            try {
                chunkAppender.reset(appenderBuffer, true);
                KeyChunkValueUtil.reset(chunkAppender, chunkKeyBuilder, chunkValBuilder, originTuple, originKeyFields,
                        chunksize);
                chunkAccessor.reset(chunkAppender.getBuffer());
                for (int j = 0; j < chunkAccessor.getTupleCount(); ++j) {
                    chunkTuple.reset(chunkAccessor, j);
                    bulkLoader.add(chunkTuple);
                }
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            bulkLoader.end();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            indexHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
    }
}
