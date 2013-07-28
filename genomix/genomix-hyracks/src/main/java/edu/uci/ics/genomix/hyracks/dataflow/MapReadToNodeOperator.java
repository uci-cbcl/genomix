/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.genomix.hyracks.dataflow;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.genomix.hyracks.data.primitive.NodeReference;
import edu.uci.ics.genomix.oldtype.PositionListWritable;
import edu.uci.ics.genomix.oldtype.PositionWritable;
import edu.uci.ics.genomix.oldtype.KmerBytesWritable;
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

    public MapReadToNodeOperator(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc, int kmerSize,
            boolean bMergeNode) {
        super(spec, 1, 1);
        recordDescriptors[0] = outRecDesc;
        this.kmerSize = kmerSize;
        this.DoMergeNodeInRead = bMergeNode;
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
    public static final int OutputForwardForwardField = 2;
    public static final int OutputForwardReverseField = 3;
    public static final int OutputReverseForwardField = 4;
    public static final int OutputReverseReverseField = 5;
    public static final int OutputKmerBytesField = 6;

    public final boolean DoMergeNodeInRead;

    public static final RecordDescriptor nodeOutputRec = new RecordDescriptor(new ISerializerDeserializer[7]);

    /**
     * (ReadID, Storage[posInRead]={len, PositionList, len, Kmer})
     * to (Position, LengthCount, InComingPosList, OutgoingPosList, Kmer)
     */
    public class MapReadToNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
        public static final int INT_LENGTH = 4;
        private final IHyracksTaskContext ctx;
        private final RecordDescriptor inputRecDesc;
        private final RecordDescriptor outputRecDesc;

        private final int LAST_POSITION_ID;

        private FrameTupleAccessor accessor;
        private ByteBuffer writeBuffer;
        private ArrayTupleBuilder builder;
        private FrameTupleAppender appender;

        private NodeReference curNodeEntry;
        private NodeReference nextNodeEntry;
        private NodeReference nextNextNodeEntry;

        private PositionListWritable cachePositionList;

        public MapReadToNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc,
                RecordDescriptor outputRecDesc) {
            this.ctx = ctx;
            this.inputRecDesc = inputRecDesc;
            this.outputRecDesc = outputRecDesc;
            curNodeEntry = new NodeReference(kmerSize);
            nextNodeEntry = new NodeReference(kmerSize);
            nextNextNodeEntry = new NodeReference(0);
            cachePositionList = new PositionListWritable();
            LAST_POSITION_ID = (inputRecDesc.getFieldCount() - InputInfoFieldStart) / 2; //?????????正负
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
            if ((accessor.getFieldCount() - InputInfoFieldStart) % 2 != 0) {
                throw new IllegalArgumentException("field length is odd");
            }

            resetNode(curNodeEntry, readID, (byte) (1));
            setForwardIncomingList(curNodeEntry,
                    offsetPoslist + accessor.getFieldStartOffset(tIndex, InputInfoFieldStart));
            setKmer(curNodeEntry.getKmer(), offsetPoslist + accessor.getFieldStartOffset(tIndex, InputInfoFieldStart));
            if (curNodeEntry.getNodeID().getPosInRead() == LAST_POSITION_ID) {
                setReverseIncomingList(curNodeEntry,
                        offsetPoslist + accessor.getFieldStartOffset(tIndex, InputInfoFieldStart + 1));
            }

            // next Node
            readNodesInfo(tIndex, readID, curNodeEntry, nextNodeEntry, InputInfoFieldStart);

            for (int i = InputInfoFieldStart + 2; i < accessor.getFieldCount(); i += 2) {
                readNodesInfo(tIndex, readID, nextNodeEntry, nextNextNodeEntry, i);

                if (!DoMergeNodeInRead || curNodeEntry.inDegree() > 1 || curNodeEntry.outDegree() > 0
                        || nextNodeEntry.inDegree() > 0 || nextNodeEntry.outDegree() > 0
                        || nextNextNodeEntry.inDegree() > 0 || nextNextNodeEntry.outDegree() > 0) {
                    connect(curNodeEntry, nextNodeEntry);
                    outputNode(curNodeEntry);
                    curNodeEntry.set(nextNodeEntry);
                    nextNodeEntry.set(nextNextNodeEntry);
                    continue;
                }
                curNodeEntry.mergeForwardNext(nextNodeEntry, kmerSize);
                nextNodeEntry.set(nextNextNodeEntry);
            }
            outputNode(curNodeEntry);
        }

        private void readNodesInfo(int tIndex, int readID, NodeReference curNode, NodeReference nextNode, int curFieldID) {
            // nextNext node
            int offsetPoslist = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength();
            if (curFieldID + 2 < accessor.getFieldCount()) {
                setForwardOutgoingList(curNode, offsetPoslist + accessor.getFieldStartOffset(tIndex, curFieldID + 2));
                resetNode(nextNode, readID, (byte) (1 + (curFieldID + 2 - InputInfoFieldStart) / 2));
                setKmer(nextNode.getKmer(), offsetPoslist + accessor.getFieldStartOffset(tIndex, curFieldID + 2));
                setReverseOutgoingList(nextNode, offsetPoslist + accessor.getFieldStartOffset(tIndex, curFieldID + 1));
                if (nextNode.getNodeID().getPosInRead() == LAST_POSITION_ID) {
                    setReverseIncomingList(nextNode,
                            offsetPoslist + accessor.getFieldStartOffset(tIndex, curFieldID + 3));
                }
            } else {
                resetNode(nextNode, readID, (byte) 0);
            }
        }

        private void setKmer(KmerBytesWritable kmer, int offset) {
            ByteBuffer buffer = accessor.getBuffer();
            int length = buffer.getInt(offset);
            offset += INT_LENGTH + length;
            length = buffer.getInt(offset);
            if (kmer.getLength() != length) {
                throw new IllegalArgumentException("kmer kmerByteSize is invalid");
            }
            offset += INT_LENGTH;
            kmer.set(buffer.array(), offset);
        }

        private void connect(NodeReference curNode, NodeReference nextNode) {
            curNode.getFFList().append(nextNode.getNodeID());
            nextNode.getRRList().append(curNode.getNodeID());
        }

        private void setCachList(int offset) {
            ByteBuffer buffer = accessor.getBuffer();
            int count = PositionListWritable.getCountByDataLength(buffer.getInt(offset));
            cachePositionList.set(count, buffer.array(), offset + INT_LENGTH);
        }

        private void resetNode(NodeReference node, int readID, byte posInRead) {
            node.reset(kmerSize);
            node.setNodeID(readID, posInRead);
        }

        private void setReverseOutgoingList(NodeReference node, int offset) {
            setCachList(offset);
            for (int i = 0; i < cachePositionList.getCountOfPosition(); i++) {
                PositionWritable pos = cachePositionList.getPosition(i);
                if (pos.getPosInRead() > 0) {
                    node.getRFList().append(pos);
                } else {
                    node.getRRList().append(pos.getReadID(), (byte) -pos.getPosInRead());
                }
            }
        }

        private void setReverseIncomingList(NodeReference node, int offset) {
            setCachList(offset);
            for (int i = 0; i < cachePositionList.getCountOfPosition(); i++) {
                PositionWritable pos = cachePositionList.getPosition(i);
                if (pos.getPosInRead() > 0) {
                    if (pos.getPosInRead() > 1) {
                        node.getFRList().append(pos.getReadID(), (byte) (pos.getPosInRead() - 1));
                    } else {
                        throw new IllegalArgumentException("Invalid position");
                    }
                } else {
                    if (pos.getPosInRead() > -LAST_POSITION_ID) {
                        node.getFFList().append(pos.getReadID(), (byte) -(pos.getPosInRead() - 1));
                    }
                }
            }
        }

        private void setForwardOutgoingList(NodeReference node, int offset) {
            setCachList(offset);
            for (int i = 0; i < cachePositionList.getCountOfPosition(); i++) {
                PositionWritable pos = cachePositionList.getPosition(i);
                if (pos.getPosInRead() > 0) {
                    node.getFFList().append(pos);
                } else {
                    node.getFRList().append(pos.getReadID(), (byte) -pos.getPosInRead());
                }
            }
        }

        private void setForwardIncomingList(NodeReference node, int offset) {
            setCachList(offset);
            for (int i = 0; i < cachePositionList.getCountOfPosition(); i++) {
                PositionWritable pos = cachePositionList.getPosition(i);
                if (pos.getPosInRead() > 0) {
                    if (pos.getPosInRead() > 1) {
                        node.getRRList().append(pos.getReadID(), (byte) (pos.getPosInRead() - 1));
                    } else {
                        throw new IllegalArgumentException("position id is invalid");
                    }
                } else {
                    if (pos.getPosInRead() > -LAST_POSITION_ID) {
                        node.getRFList().append(pos.getReadID(), (byte) -(pos.getPosInRead() - 1));
                    }
                }
            }
        }

        private void outputNode(NodeReference node) throws HyracksDataException {
            if (node.getNodeID().getPosInRead() == 0) {
                return;
            }
            try {
                builder.reset();
                builder.addField(node.getNodeID().getByteArray(), node.getNodeID().getStartOffset(), node.getNodeID()
                        .getLength());
                builder.getDataOutput().writeInt(node.getCount());
                builder.addFieldEndOffset();
                builder.addField(node.getFFList().getByteArray(), node.getFFList().getStartOffset(), node.getFFList()
                        .getLength());
                builder.addField(node.getFRList().getByteArray(), node.getFRList().getStartOffset(), node.getFRList()
                        .getLength());
                builder.addField(node.getRFList().getByteArray(), node.getRFList().getStartOffset(), node.getRFList()
                        .getLength());
                builder.addField(node.getRRList().getByteArray(), node.getRRList().getStartOffset(), node.getRRList()
                        .getLength());
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
        return new MapReadToNodePushable(ctx, recordDescProvider.getInputRecordDescriptor(getActivityId(), 0),
                recordDescriptors[0]);
    }

}
