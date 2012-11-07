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

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class RunMergingShufflerFrameReader implements IFrameReader {

    private final IHyracksTaskContext ctx;
    private final IFrameReader[] runCursors;
    private final List<ByteBuffer> inFrames;
    private final RecordDescriptor recordDesc;
    private final FrameTupleAppender outFrameAppender;

    private FrameTupleAccessor[] tupleAccessors;
    private int[] tupleFrameIndex;
    private final Random rand;

    public RunMergingShufflerFrameReader(IHyracksTaskContext ctx, IFrameReader[] runCursors, RecordDescriptor recordDesc) {
        this.ctx = ctx;
        this.runCursors = runCursors;
        this.inFrames = new ArrayList<ByteBuffer>();
        this.recordDesc = recordDesc;
        outFrameAppender = new FrameTupleAppender(ctx.getFrameSize());
        rand = new Random();
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameReader#open()
     */
    @Override
    public void open() throws HyracksDataException {
        tupleAccessors = new FrameTupleAccessor[runCursors.length];
        tupleFrameIndex = new int[runCursors.length];
        for (int i = 0; i < runCursors.length; i++) {
            runCursors[i].open();
            while (inFrames.size() < i + 1) {
                inFrames.add(ctx.allocateFrame());
            }
            if (runCursors[i].nextFrame(inFrames.get(i))) {
                tupleAccessors[i] = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
                tupleAccessors[i].reset(inFrames.get(i));
            } else {
                runCursors[i].close();
                runCursors[i] = null;
                tupleAccessors[i] = null;
            }

        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameReader#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        outFrameAppender.reset(buffer, true);
        while (hasMore()) {
            int picked = rand.nextInt(tupleAccessors.length);
            while (tupleAccessors[picked] == null) {
                picked = rand.nextInt(tupleAccessors.length);
            }

            if (!outFrameAppender.append(tupleAccessors[picked], tupleFrameIndex[picked])) {
                return true;
            }

            tupleFrameIndex[picked]++;
            if (tupleFrameIndex[picked] >= tupleAccessors[picked].getTupleCount()) {
                if (!runCursors[picked].nextFrame(inFrames.get(picked))) {
                    // run is exhausted
                    runCursors[picked].close();
                    runCursors[picked] = null;
                    tupleAccessors[picked] = null;
                } else {
                    tupleFrameIndex[picked] = 0;
                }
            }

        }

        if (outFrameAppender.getTupleCount() > 0) {
            return true;
        }

        return false;
    }

    private boolean hasMore() {
        for (int i = 0; i < tupleAccessors.length; i++) {
            if (tupleAccessors[i] != null) {
                return true;
            }
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameReader#close()
     */
    @Override
    public void close() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

}
