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

package edu.uci.ics.genomix.hyracks.contrail.graph;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.hyracks.data.primitive.PositionReference;
import edu.uci.ics.genomix.hyracks.dataflow.MapKmerPositionToReadOperator;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerListWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class AggregateKmerAggregateFactory implements IAggregatorDescriptorFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private PositionListWritable nodeIdList = new PositionListWritable();
    private KmerListWritable forwardForwardList = new KmerListWritable(kmerSize);//怎么得到kmersize
    private KmerListWritable forwardReverseList = new KmerListWritable(kmerSize);
    private KmerListWritable reverseForwardList = new KmerListWritable(kmerSize);
    private KmerListWritable reverseReverseList = new KmerListWritable(kmerSize);
    private KmerBytesWritable kmer  = new KmerBytesWritable(kmerSize);
    
    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults)
            throws HyracksDataException {
        return new IAggregatorDescriptor() {
            private NodeWritable nodeAggreter = new NodeWritable();// 能否写在外面

            protected int getOffSet(IFrameTupleAccessor accessor, int tIndex, int fieldId) {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, fieldId);
                int offset = tupleOffset + fieldStart + accessor.getFieldSlotsLength();
                return offset;
            }

            @Override
            public void reset() {
            }

            @Override
            public void close() {

            }

            @Override
            public AggregateState createAggregateStates() {
                return new AggregateState(new NodeWritable());
            }

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                NodeWritable inputVal = (NodeWritable) state.state;
                inputVal.reset(kmerSize);
                nodeIdList.reset();
                forwardForwardList.reset(kmerSize);
                forwardReverseList.reset(kmerSize);
                reverseForwardList.reset(kmerSize);
                reverseReverseList.reset(kmerSize);
                
                kmer.set(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 1));//??从1算起？？
                nodeIdList.setNewReference(1, accessor.getBuffer().array(), getOffSet(accessor, tIndex, 2));
                int ffCount = Marshal.getInt(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 3));//??
                forwardForwardList.setNewReference(ffCount, accessor.getBuffer().array(), getOffSet(accessor, tIndex, 4));
                int frCount = Marshal.getInt(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 5));
                forwardReverseList.setNewReference(frCount, accessor.getBuffer().array(), getOffSet(accessor, tIndex, 6));
                int rfCount = Marshal.getInt(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 7));
                reverseForwardList.setNewReference(frCount, accessor.getBuffer().array(), getOffSet(accessor, tIndex, 8));
                int rrCount = Marshal.getInt(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 9));
                reverseForwardList.setNewReference(rrCount, accessor.getBuffer().array(), getOffSet(accessor, tIndex, 10));
                nodeAggreter.set(nodeIdList, forwardForwardList, forwardReverseList, reverseForwardList, reverseForwardList, kmer);
                
                inputVal.getKmer().set(kmer);
                inputVal.getNodeIdList().appendList(nodeIdList);
                inputVal.getFFList().appendList(forwardForwardList);
                inputVal.getFRList().appendList(forwardReverseList);
                inputVal.getRFList().appendList(reverseForwardList);
                inputVal.getRRList().appendList(reverseReverseList);
                
                // make an empty field
                tupleBuilder.addFieldEndOffset();///????为啥
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                NodeWritable inputVal = (NodeWritable) state.state;
                kmer.set(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 1));//??从1算起？？
                nodeIdList.setNewReference(1, accessor.getBuffer().array(), getOffSet(accessor, tIndex, 2));
                int ffCount = Marshal.getInt(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 3));//??
                forwardForwardList.setNewReference(ffCount, accessor.getBuffer().array(), getOffSet(accessor, tIndex, 4));
                int frCount = Marshal.getInt(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 5));
                forwardReverseList.setNewReference(frCount, accessor.getBuffer().array(), getOffSet(accessor, tIndex, 6));
                int rfCount = Marshal.getInt(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 7));
                reverseForwardList.setNewReference(frCount, accessor.getBuffer().array(), getOffSet(accessor, tIndex, 8));
                int rrCount = Marshal.getInt(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 9));
                reverseForwardList.setNewReference(rrCount, accessor.getBuffer().array(), getOffSet(accessor, tIndex, 10));
                nodeAggreter.set(nodeIdList, forwardForwardList, forwardReverseList, reverseForwardList, reverseForwardList, kmer);
                
                inputVal.getKmer().set(kmer);
                inputVal.getNodeIdList().appendList(nodeIdList);
                inputVal.getFFList().appendList(forwardForwardList);
                inputVal.getFRList().appendList(forwardReverseList);
                inputVal.getRFList().appendList(reverseForwardList);
                inputVal.getRRList().appendList(reverseReverseList);
            }

            @Override
            public void outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                throw new IllegalStateException("partial result method should not be called");
            }

            @Override
            public void outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                DataOutput fieldOutput = tupleBuilder.getDataOutput();
                NodeWritable inputVal = (NodeWritable) state.state;
                try {
//                    fieldOutput.write(inputVal.getByteArray(), inputVal.getStartOffset(), inputVal.getLength());

                    tupleBuilder.addFieldEndOffset();// --------------为什么？
                    //-------------------------------------------------------
//                    tupleBuilder.reset();
                    fieldOutput.write(inputVal.getKmer().getBytes(), inputVal.getKmer().getOffset(), inputVal.getKmer().getLength());
                    
                    tupleBuilder.addField(Node.getreadId().getByteArray(), Node.getreadId().getStartOffset(), Node.getreadId().getLength());
                    
                    tupleBuilder.getDataOutput().writeInt(Node.getFFList().getCountOfPosition());
                    tupleBuilder.addFieldEndOffset();
                    
                    tupleBuilder.addField(Node.getFFList().getByteArray(), Node.getFFList().getStartOffset(), Node.getFFList()
                            .getLength());
                    
                    tupleBuilder.getDataOutput().writeInt(Node.getFRList().getCountOfPosition());
                    tupleBuilder.addFieldEndOffset();
                    
                    tupleBuilder.addField(Node.getFRList().getByteArray(), Node.getFRList().getStartOffset(), Node.getFRList()
                            .getLength());
                    
                    tupleBuilder.getDataOutput().writeInt(Node.getRFList().getCountOfPosition());
                    tupleBuilder.addFieldEndOffset();
                    
                    tupleBuilder.addField(Node.getRFList().getByteArray(), Node.getRFList().getStartOffset(), Node.getRFList()
                            .getLength());
                    
                    tupleBuilder.getDataOutput().writeInt(Node.getRRList().getCountOfPosition());
                    tupleBuilder.addFieldEndOffset();
                    
                    tupleBuilder.addField(Node.getRRList().getByteArray(), Node.getRRList().getStartOffset(), Node.getRRList()
                            .getLength());

/*                    if (!outputAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                            tupleBuilder.getSize())) {
                        FrameUtils.flushFrame(outputBuffer, writer);
                        outputAppender.reset(outputBuffer, true);
                        if (!outputAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                tupleBuilder.getSize())) {
                            throw new IllegalStateException(
                                    "Failed to copy an record into a frame: the record kmerByteSize is too large.");
                        }
                    }*/

                } catch (IOException e) {
                    throw new HyracksDataException("I/O exception when writing aggregation to the output buffer.");
                }
            }

        };
    }

}
