package edu.uci.ics.pregelix.dataflow.std;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexInsertUpdateDeleteOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexNonExistentKeyException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.pregelix.dataflow.util.KeyChunkTupleReference;
import edu.uci.ics.pregelix.dataflow.util.KeyChunkValueFrameTupleAppender;
import edu.uci.ics.pregelix.dataflow.util.KeyChunkValueUtil;

public class KeyChunkValueIndexInsertUpdateDeleteOperatorNodePushable extends
        IndexInsertUpdateDeleteOperatorNodePushable {

    private final int[] originKeyFields;
    private KeyChunkValueFrameTupleAppender chunkAppender;
    private ArrayTupleBuilder chunkKeyBuilder;
    private ArrayTupleBuilder chunkValBuilder;
    private FrameTupleAccessor chunkAccessor;
    private FrameTupleReference chunkTuple = new FrameTupleReference();
    private final int chunksize;
    private ByteBuffer appenderBuffer;
    private KeyChunkTupleReference deleteKeyChunkTuple;

    public KeyChunkValueIndexInsertUpdateDeleteOperatorNodePushable(IIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, int[] fieldPermutation,
            IRecordDescriptorProvider recordDescProvider, IndexOperation op, int chunksize) {
        super(opDesc, ctx, partition, fieldPermutation, recordDescProvider, op);
        this.chunksize = chunksize;
        this.originKeyFields = fieldPermutation;
        this.chunkAppender = new KeyChunkValueFrameTupleAppender(ctx.getFrameSize());
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] sd = new ISerializerDeserializer[originKeyFields.length + 2];
        this.chunkAccessor = new FrameTupleAccessor(ctx.getFrameSize(), new RecordDescriptor(sd));
        this.chunkKeyBuilder = new ArrayTupleBuilder(originKeyFields.length);
        this.chunkValBuilder = new ArrayTupleBuilder(1);
        this.deleteKeyChunkTuple = new KeyChunkTupleReference(fieldPermutation, (short) 0);
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        appenderBuffer = ctx.allocateFrame();
        appenderBuffer.position(0);
        appenderBuffer.limit(appenderBuffer.capacity());
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            try {
                if (tupleFilter != null) {
                    frameTuple.reset(accessor, i);
                    if (!tupleFilter.accept(frameTuple)) {
                        continue;
                    }
                }
                tuple.reset(accessor, i);

                switch (op) {
                    case INSERT: {
                        chunkAppender.reset(appenderBuffer, true);
                        KeyChunkValueUtil.reset(chunkAppender, chunkKeyBuilder, chunkValBuilder, tuple,
                                originKeyFields, chunksize);
                        chunkAccessor.reset(chunkAppender.getBuffer());
                        for (int j = 0; j < chunkAccessor.getTupleCount(); ++j) {
                            chunkTuple.reset(chunkAccessor, j);
                            try {
                                indexAccessor.insert(chunkTuple);
                            } catch (TreeIndexDuplicateKeyException e) {
                                // ingnore that exception to allow inserting existing keys which becomes an NoOp
                            }
                        }
                        break;
                    }
                    case DELETE: {
                        deleteKeyChunkTuple.reset(tuple);
                        while (true) {
                            try {
                                indexAccessor.delete(deleteKeyChunkTuple);
                            } catch (TreeIndexNonExistentKeyException e) {
                                // ingnore that exception to allow deletions of non-existing keys
                                break;
                            }
                            deleteKeyChunkTuple.increaseChunk();
                        }
                        break;
                    }
                    case UPDATE:
                        //TODO
                    case UPSERT:
                        //TODO
                    default: {
                        throw new HyracksDataException("Unsupported operation " + op
                                + " in tree index InsertUpdateDelete operator");
                    }
                }
            } catch (HyracksDataException e) {
                throw e;
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }
        // Pass a copy of the frame to next op.
        System.arraycopy(buffer.array(), 0, writeBuffer.array(), 0, buffer.capacity());
        FrameUtils.flushFrame(writeBuffer, writer);
    }

}
