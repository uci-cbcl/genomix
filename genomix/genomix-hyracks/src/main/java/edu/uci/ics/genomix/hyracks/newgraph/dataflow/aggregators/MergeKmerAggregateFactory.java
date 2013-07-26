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

package edu.uci.ics.genomix.hyracks.newgraph.dataflow.aggregators;

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hsqldb.lib.Iterator;

import edu.uci.ics.genomix.hyracks.data.primitive.PositionReference;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerListWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class MergeKmerAggregateFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(MergeKmerAggregateFactory.class);
    
    private final int readLength;
    private final int kmerSize;
    
    public MergeKmerAggregateFactory(int readlength, int k) {
        this.readLength = readlength;
        this.kmerSize = k;
    }
    
    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults)
            throws HyracksDataException {
        final int frameSize = ctx.getFrameSize();
        return new IAggregatorDescriptor() {

//            private PositionReference position = new PositionReference();
            
            private NodeWritable readNode = new NodeWritable(kmerSize);
            private HashSet set = new HashSet();
            private PositionListWritable uniNodeIdList = new PositionListWritable();
            private KmerListWritable uniEdgeList = new KmerListWritable(kmerSize);
            private KmerBytesWritable tempKmer = new KmerBytesWritable(kmerSize);
            private PositionWritable tempPos = new PositionWritable();
            
            @Override
            public AggregateState createAggregateStates() {
                return new AggregateState(new NodeWritable());
            }

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                NodeWritable localUniNode = (NodeWritable) state.state;
                localUniNode.reset(kmerSize);
                int leadOffset = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength();
                for (int offset = accessor.getFieldStartOffset(tIndex, 1); offset < accessor.getFieldEndOffset(tIndex,
                        1); offset += PositionReference.LENGTH) {
                    readNode.setNewReference(accessor.getBuffer().array(), leadOffset + offset);
                    localUniNode.getNodeIdList().appendList(readNode.getNodeIdList());
                    localUniNode.getFFList().appendList(readNode.getFFList());
                    localUniNode.getFRList().appendList(readNode.getFRList());
                    localUniNode.getRFList().appendList(readNode.getRFList());
                    localUniNode.getRRList().appendList(readNode.getRRList());
                }
                //make a fake feild to cheat caller
                tupleBuilder.addFieldEndOffset();
            }

            @Override
            public void reset() {

            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                NodeWritable localUniNode = (NodeWritable) state.state;
                int leadOffset = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength();
                for (int offset = accessor.getFieldStartOffset(tIndex, 1); offset < accessor.getFieldEndOffset(tIndex,
                        1); offset += PositionReference.LENGTH) {
                    position.setNewReference(accessor.getBuffer().array(), leadOffset + offset);
                    localUniNode.getNodeIdList().appendList(readNode.getNodeIdList());
                    localUniNode.getFFList().appendList(readNode.getFFList());
                    localUniNode.getFRList().appendList(readNode.getFRList());
                    localUniNode.getRFList().appendList(readNode.getRFList());
                    localUniNode.getRRList().appendList(readNode.getRRList());
                }
            }

            @Override
            public void outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                throw new IllegalStateException("partial result method should not be called");
            }

            @SuppressWarnings("unchecked")
            @Override
            public void outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                DataOutput fieldOutput = tupleBuilder.getDataOutput();
                NodeWritable localUniNode = (NodeWritable) state.state;
                uniNodeIdList.reset();
                for(java.util.Iterator<PositionWritable> iter = localUniNode.getNodeIdList().iterator(); iter.hasNext();){
                    tempPos.set(iter.next());
                    if(set.add(tempPos))
                        uniNodeIdList.append(tempPos);
                }
                localUniNode.getNodeIdList().reset();
                localUniNode.getNodeIdList().set(uniNodeIdList);
                uniEdgeList.reset();
                for(java.util.Iterator<KmerBytesWritable> iter = localUniNode.getFFList().iterator(); iter.hasNext();){
                    tempKmer.set(iter.next());
                    if(set.add(tempKmer))
                        uniEdgeList.append(tempKmer);
                }
                localUniNode.getFFList().reset();
                localUniNode.getFFList().set(uniEdgeList);
                
                uniEdgeList.reset();
                for(java.util.Iterator<KmerBytesWritable> iter = localUniNode.getFRList().iterator(); iter.hasNext();){
                    tempKmer.set(iter.next());
                    if(set.add(tempKmer))
                        uniEdgeList.append(tempKmer);
                }
                localUniNode.getFRList().reset();
                localUniNode.getFRList().set(uniEdgeList);
                
                uniEdgeList.reset();
                for(java.util.Iterator<KmerBytesWritable> iter = localUniNode.getRFList().iterator(); iter.hasNext();){
                    tempKmer.set(iter.next());
                    if(set.add(tempKmer))
                        uniEdgeList.append(tempKmer);
                }
                localUniNode.getRFList().reset();
                localUniNode.getRFList().set(uniEdgeList);
                
                uniEdgeList.reset();
                for(java.util.Iterator<KmerBytesWritable> iter = localUniNode.getRRList().iterator(); iter.hasNext();){
                    tempKmer.set(iter.next());
                    if(set.add(tempKmer))
                        uniEdgeList.append(tempKmer);
                }
                localUniNode.getRRList().reset();
                localUniNode.getRRList().set(uniEdgeList);
                
                try {
                    if (localUniNode.getLength() > frameSize / 2) {
                        LOG.warn("MergeKmer: output data kmerByteSize is too big: " + inputVal.getLength());
                    }
                    fieldOutput.write(localUniNode.getByteArray(), localUniNode.getStartOffset(), localUniNode.getLength());
                    tupleBuilder.addFieldEndOffset();

                } catch (IOException e) {
                    throw new HyracksDataException("I/O exception when writing aggregation to the output buffer.");
                }
            }

            @Override
            public void close() {

            }

        };

    }
}
