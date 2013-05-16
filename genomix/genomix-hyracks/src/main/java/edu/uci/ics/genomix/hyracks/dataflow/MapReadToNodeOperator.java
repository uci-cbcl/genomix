package edu.uci.ics.genomix.hyracks.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.genomix.hyracks.data.primitive.NodeReference;
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

    public MapReadToNodeOperator(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc) {
        super(spec, 1, 1);
        recordDescriptors[0] = outRecDesc;
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public static final int InputReadIDField = 0;
    public static final int InputInfoFieldStart = 1;

    public static final int OutputNodeIDField = 0;
    public static final int OutputCountOfKmerField = 1;
    public static final int OutputIncomingField = 2;
    public static final int OutputOutgoingField = 3;
    public static final int OutputKmerBytesField = 4;

    /**
     * (ReadID, Storage[posInRead]={PositionList,Kmer})
     * to Position, LengthCount, InComingPosList, OutgoingPosList, Kmer
     */
    public class MapReadToNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
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
                RecordDescriptor outputRecDesc) {
            this.ctx = ctx;
            this.inputRecDesc = inputRecDesc;
            this.outputRecDesc = outputRecDesc;
            curNodeEntry = new NodeReference();
            nextNodeEntry = new NodeReference();
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

        private void generateNodeFromRead(int tIndex) {
            int offsetPoslist = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength();
            resetNode(curNodeEntry, offsetPoslist + accessor.getFieldStartOffset(tIndex, InputInfoFieldStart));

            for (int i = InputInfoFieldStart + 1; i < accessor.getFieldCount(); i++) {
                setNodeRef(nextNodeEntry, offsetPoslist + accessor.getFieldStartOffset(tIndex, i));
                if (nextNodeEntry.getOutgoingList().getCountOfPosition() == 0) {
                    curNodeEntry.mergeNextWithinOneRead(nextNodeEntry);
                } else {
                    curNodeEntry.setOutgoingList(nextNodeEntry.getOutgoingList());
                    curNodeEntry.getOutgoingList().append(nextNodeEntry.getNodeID());
                    outputNode(curNodeEntry);
                    nextNodeEntry.getIncomingList().append(curNodeEntry.getNodeID());
                    curNodeEntry.set( nextNodeEntry);
                }
            }
            outputNode(curNodeEntry);
        }

        private void outputNode(NodeReference node) {
            // TODO Auto-generated method stub
            
        }

        private void setNodeRef(NodeReference node, int i) {
            // TODO Auto-generated method stub
            
        }

        private void resetNode(NodeReference node, int i) {
            
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
