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

package edu.uci.ics.genomix.hyracks.graph.dataflow.aggregators;

import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumSet;
import java.util.logging.Logger;

import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.Node.EDGETYPE;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class AggregateKmerAggregateFactory implements IAggregatorDescriptorFactory {

    /**
     * local Aggregate
     */
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = Logger.getLogger(AggregateKmerAggregateFactory.class.getName());

    public AggregateKmerAggregateFactory(int k) {
    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults)
            throws HyracksDataException {
        final int frameSize = ctx.getFrameSize();
        return new IAggregatorDescriptor() {

            private Node readNode = new Node();

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
                return new AggregateState(new Node());
            }

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                Node localUniNode = (Node) state.state;
                localUniNode.reset();
                readNode.setAsReference(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 1));

                for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
                    localUniNode.getEdgeList(e).unionUpdate((readNode.getEdgeList(e)));
                }
                localUniNode.getStartReads().addAll(readNode.getStartReads());
                localUniNode.getEndReads().addAll(readNode.getEndReads());
                localUniNode.addCoverage(readNode); // TODO: should be renamed as updateCoverage ?
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {

                Node localUniNode = (Node) state.state;

                readNode.setAsReference(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 1));
                for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
                    localUniNode.getEdgeList(e).unionUpdate(readNode.getEdgeList(e));
                }
                localUniNode.getStartReads().addAll(readNode.getStartReads());
                localUniNode.getEndReads().addAll(readNode.getEndReads());
                localUniNode.addCoverage(readNode);
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
                Node localUniNode = (Node) state.state;
                try {
                    fieldOutput.write(localUniNode.marshalToByteArray(), 0, localUniNode.getSerializedLength());
                    tupleBuilder.addFieldEndOffset();
                    if (localUniNode.getSerializedLength() > frameSize / 2) {
                        LOG.warning("Aggregate Kmer: output data kmerByteSize is too big: "
                                + localUniNode.getSerializedLength());
                    }
                } catch (IOException e) {
                    throw new HyracksDataException("I/O exception when writing aggregation to the output buffer.");
                }
            }
        };
    }
}
