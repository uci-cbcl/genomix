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
package edu.uci.ics.hyracks.dataflow.std.group.preclustered;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class PreclusteredGroupWriter implements IFrameWriter {

    private final static int INT_SIZE = 4;

    private final int[] groupFields;
    private final IBinaryComparator[] comparators;
    private final IAggregatorDescriptor aggregator;
    private final AggregateState aggregateState;
    private final IFrameWriter writer;
    private final FrameTupleAccessor inFrameAccessor;

    private final ByteBuffer outFrame;
    private final FrameTupleAppender appender;
    private final ArrayTupleBuilder tupleBuilder;

    private final RecordDescriptor outRecordDesc;

    private byte[] groupResultCache;
    private ByteBuffer groupResultCacheBuffer;
    private FrameTupleAccessor groupResultCacheAccessor;
    private FrameTupleAppender groupResultCacheAppender;

    public PreclusteredGroupWriter(IHyracksTaskContext ctx, int[] groupFields, IBinaryComparator[] comparators,
            IAggregatorDescriptor aggregator, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc,
            IFrameWriter writer) {
        this.groupFields = groupFields;
        this.comparators = comparators;
        this.aggregator = aggregator;
        this.aggregateState = aggregator.createAggregateStates();
        this.writer = writer;
        this.outRecordDesc = outRecordDesc;

        inFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecordDesc);

        outFrame = ctx.allocateFrame();
        appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(outFrame, true);

        tupleBuilder = new ArrayTupleBuilder(outRecordDesc.getFields().length);
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inFrameAccessor.reset(buffer);
        int nTuples = inFrameAccessor.getTupleCount();
        for (int i = 0; i < nTuples; ++i) {

            if (groupResultCache != null && groupResultCacheAccessor.getTupleCount() > 0) {
                groupResultCacheAccessor.reset(ByteBuffer.wrap(groupResultCache));
                if (sameGroup(inFrameAccessor, i, groupResultCacheAccessor, 0)) {
                    // find match: do aggregation
                    aggregator.aggregate(inFrameAccessor, i, groupResultCacheAccessor, 0, aggregateState);
                    continue;
                } else {
                    // write the cached group into the final output
                    writeOutput(groupResultCacheAccessor, 0);
                }
            }

            tupleBuilder.reset();

            for (int j = 0; j < groupFields.length; j++) {
                tupleBuilder.addField(inFrameAccessor, i, groupFields[j]);
            }

            aggregator.init(tupleBuilder, inFrameAccessor, i, aggregateState);

            // enlarge the cache buffer if necessary
            int requiredSize = tupleBuilder.getSize() + tupleBuilder.getFieldEndOffsets().length * INT_SIZE + 2
                    * INT_SIZE;

            if (groupResultCache == null || groupResultCache.length < requiredSize) {
                groupResultCache = new byte[requiredSize];
                groupResultCacheAppender = new FrameTupleAppender(groupResultCache.length);
                groupResultCacheBuffer = ByteBuffer.wrap(groupResultCache);
                groupResultCacheAccessor = new FrameTupleAccessor(groupResultCache.length, outRecordDesc);
            }

            groupResultCacheAppender.reset(groupResultCacheBuffer, true);
            if (!groupResultCacheAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new HyracksDataException("The partial result is too large to be initialized in a frame.");
            }
            
            groupResultCacheAccessor.reset(groupResultCacheBuffer);
        }
    }

    private void writeOutput(final FrameTupleAccessor lastTupleAccessor, int lastTupleIndex)
            throws HyracksDataException {

        tupleBuilder.reset();
        for (int j = 0; j < groupFields.length; j++) {
            tupleBuilder.addField(lastTupleAccessor, lastTupleIndex, j);
        }
        aggregator.outputFinalResult(tupleBuilder, lastTupleAccessor, lastTupleIndex, aggregateState);

        if (!appender.appendSkipEmptyField(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                tupleBuilder.getSize())) {
            FrameUtils.flushFrame(outFrame, writer);
            appender.reset(outFrame, true);
            if (!appender.appendSkipEmptyField(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }

    }

    private boolean sameGroup(FrameTupleAccessor a1, int t1Idx, FrameTupleAccessor a2, int t2Idx) {
        for (int i = 0; i < comparators.length; ++i) {
            int fIdx = groupFields[i];
            int s1 = a1.getTupleStartOffset(t1Idx) + a1.getFieldSlotsLength() + a1.getFieldStartOffset(t1Idx, fIdx);
            int l1 = a1.getFieldLength(t1Idx, fIdx);
            int s2 = a2.getTupleStartOffset(t2Idx) + a2.getFieldSlotsLength() + a2.getFieldStartOffset(t2Idx, i);
            int l2 = a2.getFieldLength(t2Idx, i);
            if (comparators[i].compare(a1.getBuffer().array(), s1, l1, a2.getBuffer().array(), s2, l2) != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        if (groupResultCache != null && groupResultCacheAccessor.getTupleCount() > 0) {
            writeOutput(groupResultCacheAccessor, 0);
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(outFrame, writer);
            }
        }
        aggregateState.close();
        writer.close();
    }
}