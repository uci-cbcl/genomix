/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.sort.FrameSorter;

public class GroupingFrameSorter extends FrameSorter {

    private final IAggregatorDescriptor aggregator;

    private final AggregateState aggregateState;

    private final RecordDescriptor partialOutRecordDescriptor, finalOutRecordDescriptor;

    // FIXME
    private static final Logger LOGGER = Logger.getLogger(GroupingFrameSorter.class.getSimpleName());

    public GroupingFrameSorter(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor partialOutRecordDescriptor, RecordDescriptor finalOutRecordDescriptor,
            IAggregatorDescriptor aggregator, AggregateState aggregateState) {
        super(ctx, sortFields, firstKeyNormalizerFactory, comparatorFactories, partialOutRecordDescriptor);
        this.aggregator = aggregator;
        this.aggregateState = aggregateState;
        this.partialOutRecordDescriptor = partialOutRecordDescriptor;
        this.finalOutRecordDescriptor = finalOutRecordDescriptor;
    }

    public void reset() {
        super.reset();
    }

    public ByteBuffer getBuffer(int bufferIndex) throws HyracksDataException {
        if (bufferIndex >= 0 && bufferIndex < dataFrameCount) {
            return buffers.get(bufferIndex);
        } else {
            throw new HyracksDataException("Frame access is out of the array boundary");
        }

    }

    public void flushFrames(IFrameWriter writer, boolean isPartial) throws HyracksDataException {
        // FIXME
        long flushTimer = System.currentTimeMillis();
        int ioFrameCounter = 0;

        ArrayTupleBuilder outTupleBuilder;
        if (isPartial) {
            outTupleBuilder = new ArrayTupleBuilder(partialOutRecordDescriptor.getFieldCount());
        } else {
            outTupleBuilder = new ArrayTupleBuilder(finalOutRecordDescriptor.getFieldCount());
        }
        appender.reset(outFrame, true);
        if (tupleCount == 0 && dataFrameCount > 0) {
            // handle left-over tuples, which are not sorted
            for (int i = 0; i < dataFrameCount; i++) {
                fta1.reset(buffers.get(i));
                int tupleCount = fta1.getTupleCount();
                for (int j = 0; j < tupleCount; j++) {
                    int tStart = fta1.getTupleStartOffset(j);
                    int tEnd = fta1.getTupleEndOffset(j);
                    outTupleBuilder.reset();

                    for (int k = 0; k < sortFields.length; k++) {
                        int fieldStart = tStart + fta1.getFieldSlotsLength();

                        int fieldOffset = 0;

                        if (sortFields[k] > 0) {
                            fieldOffset = buffers.get(i).getInt(tStart + 4 * (sortFields[k] - 1));
                        }

                        int fieldLength = buffers.get(i).getInt(tStart + 4 * sortFields[k]) - fieldOffset;
                        outTupleBuilder.addField(buffers.get(i).array(), fieldStart + fieldOffset, fieldLength);
                    }

                    if (isPartial) {
                        aggregator.outputPartialResult(outTupleBuilder, fta1.getBuffer().array(), tStart,
                                tEnd - tStart, fta1.getFieldCount(), 4, aggregateState);
                    } else {
                        aggregator.outputFinalResult(outTupleBuilder, fta1.getBuffer().array(), tStart, tEnd - tStart,
                                fta1.getFieldCount(), 4, aggregateState);
                    }

                    if (!appender.appendSkipEmptyField(outTupleBuilder.getFieldEndOffsets(),
                            outTupleBuilder.getByteArray(), 0, outTupleBuilder.getSize())) {
                        FrameUtils.flushFrame(outFrame, writer);
                        appender.reset(outFrame, true);
                        if (!appender.appendSkipEmptyField(outTupleBuilder.getFieldEndOffsets(),
                                outTupleBuilder.getByteArray(), 0, outTupleBuilder.getSize())) {
                            throw new IllegalStateException();
                        }
                    }
                }
            }
        } else {
            for (int ptr = 0; ptr < tupleCount; ++ptr) {
                int i = tPointers[ptr * 4];
                int tStart = tPointers[ptr * 4 + 1];
                int tEnd = tPointers[ptr * 4 + 2];
                ByteBuffer buffer = buffers.get(i);
                fta1.reset(buffer);

                outTupleBuilder.reset();

                for (int k = 0; k < sortFields.length; k++) {
                    int fieldStart = tStart + fta1.getFieldSlotsLength();

                    int fieldOffset = 0;

                    if (sortFields[k] > 0) {
                        fieldOffset = buffer.getInt(tStart + 4 * (sortFields[k] - 1));
                    }

                    int fieldLength = buffer.getInt(tStart + 4 * sortFields[k]) - fieldOffset;
                    outTupleBuilder.addField(buffer.array(), fieldStart + fieldOffset, fieldLength);
                }

                if (isPartial) {
                    aggregator.outputPartialResult(outTupleBuilder, fta1.getBuffer().array(), tStart, tEnd - tStart,
                            fta1.getFieldCount(), 4, aggregateState);
                } else {
                    aggregator.outputFinalResult(outTupleBuilder, fta1.getBuffer().array(), tStart, tEnd - tStart,
                            fta1.getFieldCount(), 4, aggregateState);
                }

                if (!appender.appendSkipEmptyField(outTupleBuilder.getFieldEndOffsets(),
                        outTupleBuilder.getByteArray(), 0, outTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outFrame, writer);
                    // FIXME
                    ioFrameCounter++;
                    appender.reset(outFrame, true);
                    if (!appender.appendSkipEmptyField(outTupleBuilder.getFieldEndOffsets(),
                            outTupleBuilder.getByteArray(), 0, outTupleBuilder.getSize())) {
                        throw new IllegalStateException();
                    }
                }
            }
        }
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outFrame, writer);
            // FIXME
            ioFrameCounter++;
            appender.reset(outFrame, true);
        }
        aggregator.close();

        // FIXME
        flushTimer = System.currentTimeMillis() - flushTimer;
        LOGGER.warning("GroupingFrameSorter-Flush\t" + tupleCount + "\t" + flushTimer + "\t" + ioFrameCounter);
    }

}
