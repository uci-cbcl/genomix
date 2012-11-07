/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class FrameShuffler {

    private final IHyracksCommonContext ctx;

    protected final List<ByteBuffer> buffers;

    protected final FrameTupleAccessor fta;

    protected final FrameTupleAppender appender;

    protected final ByteBuffer outFrame;

    private final Random rand;

    protected int dataFrameCount;
    protected int[] tPointers;
    protected int tupleCount;

    public FrameShuffler(IHyracksCommonContext ctx, RecordDescriptor recordDescriptor) {
        this.ctx = ctx;

        buffers = new ArrayList<ByteBuffer>();
        fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        appender = new FrameTupleAppender(ctx.getFrameSize());
        outFrame = ctx.allocateFrame();
        rand = new Random();
    }

    public void reset() {
        dataFrameCount = 0;
        tupleCount = 0;
    }

    public void close() {
        this.buffers.clear();
        tPointers = null;
    }

    public int getFrameCount() {
        return dataFrameCount;
    }

    public void insertFrame(ByteBuffer buffer) {
        ByteBuffer copyFrame;
        if (dataFrameCount == buffers.size()) {
            copyFrame = ctx.allocateFrame();
            buffers.add(copyFrame);
        } else {
            copyFrame = buffers.get(dataFrameCount);
        }
        FrameUtils.copy(buffer, copyFrame);
        ++dataFrameCount;
    }

    public void shufferFrames() {
        int nBuffers = dataFrameCount;
        tupleCount = 0;
        for (int i = 0; i < nBuffers; i++) {
            fta.reset(buffers.get(i));
            tupleCount += fta.getTupleCount();
        }
        tPointers = ((tPointers == null) || (tPointers.length < tupleCount * 3)) ? new int[tupleCount * 3] : tPointers;
        int ptr = 0;
        for (int i = 0; i < nBuffers; i++) {
            fta.reset(buffers.get(i));
            int tCount = fta.getTupleCount();
            for (int j = 0; j < tCount; ++j) {
                int tStart = fta.getTupleStartOffset(j);
                int tEnd = fta.getTupleEndOffset(j);
                tPointers[ptr * 3] = i;
                tPointers[ptr * 3 + 1] = tStart;
                tPointers[ptr * 3 + 2] = tEnd;
                ptr++;
            }
        }

        if (tupleCount > 0) {
            shuffle(0, tupleCount);
        }
    }

    public void flushFrames(IFrameWriter writer) throws HyracksDataException {
        appender.reset(outFrame, true);
        for (int ptr = 0; ptr < tupleCount; ++ptr) {
            int i = tPointers[ptr * 3];
            int tStart = tPointers[ptr * 3 + 1];
            int tEnd = tPointers[ptr * 3 + 2];
            ByteBuffer buffer = buffers.get(i);
            fta.reset(buffer);
            if (!appender.append(fta, tStart, tEnd)) {
                FrameUtils.flushFrame(outFrame, writer);
                appender.reset(outFrame, true);
                if (!appender.append(fta, tStart, tEnd)) {
                    throw new IllegalStateException();
                }
            }
        }
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outFrame, writer);
        }
    }

    /**
     * Fisher-Yates shuffles for records in memory.
     * 
     * @param start
     * @param length
     */
    private void shuffle(int start, int length) {
        for (int i = start + length - 1; i > start; i--) {
            int picked = rand.nextInt(i) + start;
            swap(i, picked);
        }
    }

    private void swap(int a, int b) {
        for (int i = 0; i < 3; ++i) {
            int t = tPointers[a * 3 + i];
            tPointers[a * 3 + i] = tPointers[b * 3 + i];
            tPointers[b * 3 + i] = t;
        }
    }

}
