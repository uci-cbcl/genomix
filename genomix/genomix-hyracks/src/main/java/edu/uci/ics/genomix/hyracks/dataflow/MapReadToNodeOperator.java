package edu.uci.ics.genomix.hyracks.dataflow;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.genomix.hyracks.data.primitive.NodeReference;
import edu.uci.ics.genomix.hyracks.data.primitive.PositionReference;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class MapReadToNodeOperator extends AbstractSingleActivityOperatorDescriptor {

    public MapReadToNodeOperator(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc, int kmerSize) {
        super(spec, 1, 1);
        recordDescriptors[0] = outRecDesc;
        this.kmerSize = kmerSize;
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final int kmerSize;

    public static final int InputReadIDField = 0;
    public static final int InputInfoFieldStart = 1;

    public static final int OutputNodeIDField = 0;
    public static final int OutputCountOfKmerField = 1;
    public static final int OutputIncomingField = 2;
    public static final int OutputOutgoingField = 3;
    public static final int OutputKmerBytesField = 4;

    /**
     * (ReadID, Storage[posInRead]={len, PositionList, len, Kmer})
     * to (Position, LengthCount, InComingPosList, OutgoingPosList, Kmer)
     */
    public class MapReadToNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
        public static final int INT_LENGTH = 4;
        private final IHyracksTaskContext ctx;
        private final RecordDescriptor inputRecDesc;
        private final RecordDescriptor outputRecDesc;

        private FrameTupleAccessor accessor;
        private ByteBuffer writeBuffer;
        private ArrayTupleBuilder builder;
        private FrameTupleAppender appender;

        private NodeReference curNodeEntry;
        private NodeReference nextNodeEntry;

        public MapReadToNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc,
                RecordDescriptor outputRecDesc, int kmerSize) {
            this.ctx = ctx;
            this.inputRecDesc = inputRecDesc;
            this.outputRecDesc = outputRecDesc;
            curNodeEntry = new NodeReference(kmerSize);
            nextNodeEntry = new NodeReference(kmerSize);
        }

        @Override
        public void open() throws HyracksDataException {
            accessor = new FrameTupleAccessor(ctx.getFrameSize(), inputRecDesc);
            writeBuffer = ctx.allocateFrame();
            builder = new ArrayTupleBuilder(outputRecDesc.getFieldCount());
            appender = new FrameTupleAppender(ctx.getFrameSize());
            appender.reset(writeBuffer, true);
            writer.open();
            curNodeEntry.reset();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            accessor.reset(buffer);
            int tupleCount = accessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                generateNodeFromRead(i);
            }
        }

        private void generateNodeFromRead(int tIndex) throws HyracksDataException {
            int offsetPoslist = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength();
            int readID = accessor.getBuffer().getInt(
                    offsetPoslist + accessor.getFieldStartOffset(tIndex, InputReadIDField));
            resetNode(curNodeEntry, readID, (byte) 0,
                    offsetPoslist + accessor.getFieldStartOffset(tIndex, InputInfoFieldStart), false);

            for (int i = InputInfoFieldStart + 1; i < accessor.getFieldCount(); i++) {
                resetNode(nextNodeEntry, readID, (byte) (i - InputInfoFieldStart),
                        offsetPoslist + accessor.getFieldStartOffset(tIndex, i), true);
                if (nextNodeEntry.getOutgoingList().getCountOfPosition() == 0) {
                    curNodeEntry.mergeNextWithinOneRead(nextNodeEntry);
                } else {
                    curNodeEntry.setOutgoingList(nextNodeEntry.getOutgoingList());
                    curNodeEntry.getOutgoingList().append(nextNodeEntry.getNodeID());
                    outputNode(curNodeEntry);
                    nextNodeEntry.getIncomingList().append(curNodeEntry.getNodeID());
                    curNodeEntry.set(nextNodeEntry);
                }
            }
            outputNode(curNodeEntry);
        }

        private void outputNode(NodeReference node) throws HyracksDataException {
            try {
                builder.addField(node.getNodeID().getByteArray(), node.getNodeID().getStartOffset(), node.getNodeID()
                        .getLength());
                builder.getDataOutput().writeInt(node.getCount());
                builder.addFieldEndOffset();
                builder.addField(node.getIncomingList().getByteArray(), node.getIncomingList().getStartOffset(), node
                        .getIncomingList().getLength());
                builder.addField(node.getOutgoingList().getByteArray(), node.getOutgoingList().getStartOffset(), node
                        .getOutgoingList().getLength());
                builder.addField(node.getKmer().getBytes(), node.getKmer().getOffset(), node.getKmer().getLength());

                if (!appender.append(builder.getFieldEndOffsets(), builder.getByteArray(), 0, builder.getSize())) {
                    FrameUtils.flushFrame(writeBuffer, writer);
                    appender.reset(writeBuffer, true);
                    if (!appender.append(builder.getFieldEndOffsets(), builder.getByteArray(), 0, builder.getSize())) {
                        throw new IllegalStateException("Failed to append tuplebuilder to frame");
                    }
                }
                builder.reset();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to Add a field to the tupleBuilder.");
            }
        }

        private void resetNode(NodeReference node, int readID, byte posInRead, int offset, boolean byRef) {
            node.reset();
            node.setNodeID(readID, posInRead);

            ByteBuffer buffer = accessor.getBuffer();
            int lengthOfPosition = buffer.getInt(offset);
            if (lengthOfPosition % PositionReference.LENGTH != 0) {
                throw new IllegalStateException("Size of PositionList is invalid ");
            }
            offset += INT_LENGTH;
            if (posInRead == 0) {
                setPositionList(node.getIncomingList(), lengthOfPosition / PositionReference.LENGTH, buffer.array(),
                        offset, byRef);
            } else {
                setPositionList(node.getOutgoingList(), lengthOfPosition / PositionReference.LENGTH, buffer.array(),
                        offset, byRef);
            }
            offset += lengthOfPosition;
            int lengthKmer = buffer.getInt(offset);
            if (node.getKmer().getLength() != lengthKmer) {
                throw new IllegalStateException("Size of Kmer is invalid ");
            }
            setKmer(node.getKmer(), buffer.array(), offset + INT_LENGTH, byRef);
            node.setCount(1);
        }

        private void setKmer(KmerBytesWritable kmer, byte[] array, int offset, boolean byRef) {
            if (byRef) {
                kmer.setNewReference(array, offset);
            } else {
                kmer.set(array, offset);
            }
        }

        private void setPositionList(PositionListWritable positionListWritable, int count, byte[] array, int offset, boolean byRef) {
            if (byRef) {
                positionListWritable.setNewReference(count, array, offset);
            } else {
                positionListWritable.set(count, array, offset);
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
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        // TODO Auto-generated method stub
        return new MapReadToNodePushable(ctx, recordDescProvider.getInputRecordDescriptor(getActivityId(), 0),
                recordDescriptors[0], kmerSize);
    }

}
