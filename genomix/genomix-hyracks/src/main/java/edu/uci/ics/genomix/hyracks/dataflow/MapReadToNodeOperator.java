package edu.uci.ics.genomix.hyracks.dataflow;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.genomix.hyracks.data.primitive.NodeReference;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
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

    public static final RecordDescriptor nodeOutputRec = new RecordDescriptor(new ISerializerDeserializer[] { null,
            null, null, null, null });

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
        private NodeReference nextNextNodeEntry;

        public MapReadToNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc,
                RecordDescriptor outputRecDesc) {
            this.ctx = ctx;
            this.inputRecDesc = inputRecDesc;
            this.outputRecDesc = outputRecDesc;
            curNodeEntry = new NodeReference(kmerSize);
            nextNodeEntry = new NodeReference(kmerSize);
            nextNextNodeEntry = new NodeReference(0);
        }

        @Override
        public void open() throws HyracksDataException {
            accessor = new FrameTupleAccessor(ctx.getFrameSize(), inputRecDesc);
            writeBuffer = ctx.allocateFrame();
            builder = new ArrayTupleBuilder(outputRecDesc.getFieldCount());
            appender = new FrameTupleAppender(ctx.getFrameSize());
            appender.reset(writeBuffer, true);
            writer.open();
            curNodeEntry.reset(kmerSize);
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
                    offsetPoslist + accessor.getFieldStartOffset(tIndex, InputInfoFieldStart), true);

            for (int i = InputInfoFieldStart + 1; i < accessor.getFieldCount(); i++) {
                resetNode(nextNodeEntry, readID, (byte) (i - InputInfoFieldStart),
                        offsetPoslist + accessor.getFieldStartOffset(tIndex, i), true);
                NodeReference pNextNext = null;
                if (i + 1 < accessor.getFieldCount()) {
                    resetNode(nextNextNodeEntry, readID, (byte) (i - InputInfoFieldStart + 1),
                            offsetPoslist + accessor.getFieldStartOffset(tIndex, i + 1), false);
                    pNextNext = nextNextNodeEntry;
                }

                if (nextNodeEntry.getOutgoingList().getCountOfPosition() == 0) {
                    if (pNextNext == null || pNextNext.getOutgoingList().getCountOfPosition() == 0) {
                        curNodeEntry.mergeNext(nextNodeEntry, kmerSize);
                    } else {
                        curNodeEntry.getOutgoingList().reset();
                        curNodeEntry.getOutgoingList().append(nextNodeEntry.getNodeID());
                        outputNode(curNodeEntry);

                        nextNodeEntry.getIncomingList().append(curNodeEntry.getNodeID());
                        curNodeEntry.set(nextNodeEntry);
                    }
                } else { // nextNode entry outgoing > 0
                    curNodeEntry.getOutgoingList().set(nextNodeEntry.getOutgoingList());
                    curNodeEntry.getOutgoingList().append(nextNodeEntry.getNodeID());
                    nextNodeEntry.getIncomingList().append(curNodeEntry.getNodeID());
                    outputNode(curNodeEntry);
                    curNodeEntry.set(nextNodeEntry);
                    curNodeEntry.getOutgoingList().reset();
                }
            }
            outputNode(curNodeEntry);
        }

        private void resetNode(NodeReference node, int readID, byte posInRead, int offset, boolean isInitial) {
            node.reset(kmerSize);
            node.setNodeID(readID, posInRead);

            ByteBuffer buffer = accessor.getBuffer();
            int lengthPos = buffer.getInt(offset);
            int countPosition = PositionListWritable.getCountByDataLength(lengthPos);
            offset += INT_LENGTH;
            if (posInRead == 0) {
                setPositionList(node.getIncomingList(), countPosition, buffer.array(), offset, true);
                // minus 1 position of the incoming list to get the correct predecessor
                for (PositionWritable pos : node.getIncomingList()) {
                    if (pos.getPosInRead() == 0) {
                        for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
                            System.out.println(ste);
                        }
                        throw new IllegalArgumentException("The incoming position list contain invalid posInRead");
                    }
                    pos.set(pos.getReadID(), (byte) (pos.getPosInRead() - 1));
                }
            } else {
                setPositionList(node.getOutgoingList(), countPosition, buffer.array(), offset, isInitial);
            }
            offset += lengthPos;
            int lengthKmer = buffer.getInt(offset);
            if (node.getKmer().getLength() != lengthKmer) {
                for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
                    System.out.println(ste);
                }
                throw new IllegalStateException("Size of Kmer is invalid ");
            }
            setKmer(node.getKmer(), buffer.array(), offset + INT_LENGTH, isInitial);
        }

        private void setKmer(KmerBytesWritable kmer, byte[] array, int offset, boolean isInitial) {
            if (isInitial) {
                kmer.set(array, offset);
            } else {
                kmer.setNewReference(array, offset);
            }
        }

        private void setPositionList(PositionListWritable positionListWritable, int count, byte[] array, int offset,
                boolean isInitial) {
            if (isInitial) {
                positionListWritable.set(count, array, offset);
            } else {
                positionListWritable.setNewReference(count, array, offset);
            }
        }

        private void outputNode(NodeReference node) throws HyracksDataException {
            try {
                builder.reset();
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
            } catch (IOException e) {
                throw new IllegalStateException("Failed to Add a field to the tupleBuilder.");
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
                recordDescriptors[0]);
    }

}
