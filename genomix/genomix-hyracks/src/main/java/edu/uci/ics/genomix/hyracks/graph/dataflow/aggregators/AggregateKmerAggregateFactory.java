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
import java.util.Iterator;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;
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
    private final int kmerSize;

    public AggregateKmerAggregateFactory(int k) {
        this.kmerSize = k;
    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults)
            throws HyracksDataException {
        Kmer.setGlobalKmerLength(kmerSize);
        return new IAggregatorDescriptor() {

            private Node readNode = new Node();

            //            private KmerBytesWritable readKmer = new KmerBytesWritable();

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
                //                localUniNode.foundMe = false;
                //                localUniNode.previous = "";
                //                localUniNode.stepCount = 0;
                //                readKmer.setAsCopy(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 0));
                readNode.setAsReference(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 1));

                //                if (readKmer.toString().equals("CGAAGTATCTCGACAGCAAGTCCGTCCGTCCCAACCACGTCGACGAGCGTCGTAA")) {
                //                    Iterator<VKmerBytesWritable> it = readNode.getEdgeList(DirectionFlag.DIR_FR).getKeys();
                //                    while (it.hasNext()) {
                //                        System.out.println("---------->readNode  "
                //                                + it.next().toString());
                //                    }
                //                    if(readNode.getEdgeList(DirectionFlag.DIR_FR).getCountOfPosition() > 0 && readNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().toString().contains("11934501")) {
                //                        System.out.println("---------->localUniNode "
                //                                + localUniNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().toString());
                //                        localUniNode.foundMe = true;
                //                    }
                //                }
                for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
                    localUniNode.getEdgeMap(e).unionUpdate((readNode.getEdgeMap(e)));
                }
                localUniNode.getStartReads().addAll(readNode.getStartReads());
                localUniNode.getEndReads().addAll(readNode.getEndReads());
                localUniNode.addCoverage(readNode);

            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {

                Node localUniNode = (Node) state.state;

                //                readKmer.setAsCopy(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 0));
                readNode.setAsReference(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 1));
                for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
                    localUniNode.getEdgeMap(e).unionUpdate(readNode.getEdgeMap(e));
                }
                localUniNode.getStartReads().addAll(readNode.getStartReads());
                localUniNode.getEndReads().addAll(readNode.getEndReads());
                localUniNode.addCoverage(readNode);
                //                if (readKmer.toString().equals("CGAAGTATCTCGACAGCAAGTCCGTCCGTCCCAACCACGTCGACGAGCGTCGTAA")) {
                //                    if(readNode.getEdgeList(DirectionFlag.DIR_FR).getCountOfPosition() > 0 && readNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().toString().contains("11934501")) {                        
                //                        System.out.println("***********************************************************");
                //                        
                //                        System.out.println("---------->readNode  "
                //                                + (readNode.getEdgeList(DirectionFlag.DIR_FR).getCountOfPosition() > 0 ? readNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().toString() : "null"));
                //                        System.out.println("---------->localUniNode "
                //                                + (localUniNode.getEdgeList(DirectionFlag.DIR_FR).getCountOfPosition() > 0 ? localUniNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().toString() : "null"));
                //                        
                //                        System.out.println("-->number for FR: " + localUniNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().getCountOfPosition());
                //                        
                //                        localUniNode.foundMe = true;
                //                        localUniNode.previous = localUniNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().toString();
                //                        localUniNode.stepCount++;
                //                    } else if (localUniNode.foundMe) {
                //                        if (localUniNode.getEdgeList(DirectionFlag.DIR_FR).getCountOfPosition() > 0) {
                //                            if (localUniNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().toString().contains("11934501")) {
                //                            // good, it's still there
                //                                localUniNode.stepCount++;
                //                                localUniNode.previous = localUniNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().toString(); 
                //                            } else {
                //                                
                //                                System.out.println("-->number for FR: " + localUniNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().getCountOfPosition());
                //                                System.out.println("ERROR: the value has disappeared! previously:\n" + "stepCount: " + localUniNode.stepCount + localUniNode.previous + "\n\ncurrently:\n" + localUniNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().toString());
                //                            }
                //                        }
                //                    }
                //                }
            }

            @Override
            public void outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                throw new IllegalStateException("partial result method should not be called");
            }

            @Override
            public void outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                //                readKmer.setAsCopy(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 0));

                DataOutput fieldOutput = tupleBuilder.getDataOutput();
                Node localUniNode = (Node) state.state;
                //                if (readKmer.toString().equals("CGAAGTATCTCGACAGCAAGTCCGTCCGTCCCAACCACGTCGACGAGCGTCGTAA")) {
                //                    if(readNode.getEdgeList(DirectionFlag.DIR_FR).getCountOfPosition() > 0 && readNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().toString().contains("11934501")) {                        
                //                        System.out.println("local final output***********************************************************");
                //                        System.out.println("---------->readNode  "
                //                                + (readNode.getEdgeList(DirectionFlag.DIR_FR).getCountOfPosition() > 0 ? readNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().toString() : "null"));
                //                        System.out.println("---------->localUniNode "
                //                                + (localUniNode.getEdgeList(DirectionFlag.DIR_FR).getCountOfPosition() > 0 ? localUniNode.getEdgeList(DirectionFlag.DIR_FR).get(0).getReadIDs().toString() : "null"));
                //                    }
                //                }
                try {
                    fieldOutput.write(localUniNode.marshalToByteArray(), 0, localUniNode.getSerializedLength());
                    tupleBuilder.addFieldEndOffset();
                } catch (IOException e) {
                    throw new HyracksDataException("I/O exception when writing aggregation to the output buffer.");
                }
            }
        };
    }
}
