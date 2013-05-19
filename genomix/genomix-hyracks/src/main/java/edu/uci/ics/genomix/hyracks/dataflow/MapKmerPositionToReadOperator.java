package edu.uci.ics.genomix.hyracks.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.genomix.hyracks.data.primitive.PositionReference;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class MapKmerPositionToReadOperator extends AbstractSingleActivityOperatorDescriptor {

    public MapKmerPositionToReadOperator(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc) {
        super(spec, 1, 1);
        recordDescriptors[0] = recDesc;
    }

    private static final long serialVersionUID = 1L;
    public static final int InputKmerField = 0;
    public static final int InputPosListField = 1;

    public static final int OutputReadIDField = 0;
    public static final int OutputPosInReadField = 1;
    public static final int OutputOtherReadIDListField = 2;
    public static final int OutputKmerField = 3; // may not needed

    public static final RecordDescriptor readIDOutputRec = new RecordDescriptor(new ISerializerDeserializer[] { null,
            null, null, null });

    /**
     * Map (Kmer, {(ReadID,PosInRead),...}) into
     * (ReadID,PosInRead,{OtherReadID,...},*Kmer*) OtherReadID appears only when
     * otherReadID.otherPos==0
     */
    public class MapKmerPositionToReadNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
        private final IHyracksTaskContext ctx;
        private final RecordDescriptor inputRecDesc;
        private final RecordDescriptor outputRecDesc;

        private FrameTupleAccessor accessor;
        private ByteBuffer writeBuffer;
        private ArrayTupleBuilder builder;
        private FrameTupleAppender appender;

        private PositionReference positionEntry;
        private ArrayBackedValueStorage posListEntry;
        private ArrayBackedValueStorage zeroPositionCollection;
        private ArrayBackedValueStorage noneZeroPositionCollection;

        public MapKmerPositionToReadNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc,
                RecordDescriptor outputRecDesc) {
            this.ctx = ctx;
            this.inputRecDesc = inputRecDesc;
            this.outputRecDesc = outputRecDesc;
            this.positionEntry = new PositionReference();
            this.posListEntry = new ArrayBackedValueStorage();
            this.zeroPositionCollection = new ArrayBackedValueStorage();
            this.noneZeroPositionCollection = new ArrayBackedValueStorage();
        }

        @Override
        public void open() throws HyracksDataException {
            accessor = new FrameTupleAccessor(ctx.getFrameSize(), inputRecDesc);
            writeBuffer = ctx.allocateFrame();
            builder = new ArrayTupleBuilder(outputRecDesc.getFieldCount());
            appender = new FrameTupleAppender(ctx.getFrameSize());
            appender.reset(writeBuffer, true);
            writer.open();
            posListEntry.reset();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            accessor.reset(buffer);
            int tupleCount = accessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                scanPosition(i, zeroPositionCollection, noneZeroPositionCollection);
                scanAgainToOutputTuple(i, zeroPositionCollection, noneZeroPositionCollection, builder);
            }
        }

        private void scanPosition(int tIndex, ArrayBackedValueStorage zeroPositionCollection2,
                ArrayBackedValueStorage noneZeroPositionCollection2) {
            zeroPositionCollection2.reset();
            noneZeroPositionCollection2.reset();
            byte[] data = accessor.getBuffer().array();
            int offsetPoslist = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength()
                    + accessor.getFieldStartOffset(tIndex, InputPosListField);
            for (int i = 0; i < accessor.getFieldLength(tIndex, InputPosListField); i += PositionReference.LENGTH) {
                positionEntry.setNewReference(data, offsetPoslist + i);
                if (positionEntry.getPosInRead() == 0) {
                    zeroPositionCollection2.append(positionEntry);
                } else {
                    noneZeroPositionCollection2.append(positionEntry);
                }
            }

        }

        private void scanAgainToOutputTuple(int tIndex, ArrayBackedValueStorage zeroPositionCollection,
                ArrayBackedValueStorage noneZeroPositionCollection, ArrayTupleBuilder builder2) {
            byte[] data = accessor.getBuffer().array();
            int offsetPoslist = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength()
                    + accessor.getFieldStartOffset(tIndex, InputPosListField);
            for (int i = 0; i < accessor.getFieldLength(tIndex, InputPosListField); i += PositionReference.LENGTH) {
                positionEntry.setNewReference(data, offsetPoslist + i);
                if (positionEntry.getPosInRead() != 0) {
                    appendNodeToBuilder(tIndex, positionEntry, zeroPositionCollection, builder2);
                } else {
                    appendNodeToBuilder(tIndex, positionEntry, noneZeroPositionCollection, builder2);
                }
            }
        }

        private void appendNodeToBuilder(int tIndex, PositionReference pos, ArrayBackedValueStorage posList2,
                ArrayTupleBuilder builder2) {
            try {
                builder2.addField(pos.getByteArray(), pos.getStartOffset(), PositionReference.INTBYTES);
                builder2.addField(pos.getByteArray(), pos.getStartOffset() + PositionReference.INTBYTES, 1);
                if (posList2 == null) {
                    builder2.addFieldEndOffset();
                } else {
                    builder2.addField(posList2.getByteArray(), posList2.getStartOffset(), posList2.getLength());
                }
                // set kmer, may not useful
                byte[] data = accessor.getBuffer().array();
                int offsetKmer = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength()
                        + accessor.getFieldStartOffset(tIndex, InputKmerField);
                builder2.addField(data, offsetKmer, accessor.getFieldLength(tIndex, InputKmerField));

                if (!appender.append(builder2.getFieldEndOffsets(), builder2.getByteArray(), 0, builder2.getSize())) {
                    FrameUtils.flushFrame(writeBuffer, writer);
                    appender.reset(writeBuffer, true);
                    if (!appender.append(builder2.getFieldEndOffsets(), builder2.getByteArray(), 0, builder2.getSize())) {
                        throw new IllegalStateException();
                    }
                }
                builder2.reset();
            } catch (HyracksDataException e) {
                throw new IllegalStateException(
                        "Failed to Add a field to the tuple by copying the data bytes from a byte array.");
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            writer.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(writeBuffer, writer);
            }
            writer.close();
        }

    }

    @Override
    public AbstractOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new MapKmerPositionToReadNodePushable(ctx, recordDescProvider.getInputRecordDescriptor(getActivityId(),
                0), recordDescriptors[0]);
    }

}
