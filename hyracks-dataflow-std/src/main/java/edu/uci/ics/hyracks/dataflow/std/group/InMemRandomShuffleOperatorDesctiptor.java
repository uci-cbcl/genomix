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
import java.util.Random;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class InMemRandomShuffleOperatorDesctiptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    public InMemRandomShuffleOperatorDesctiptor(JobSpecification spec, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        recordDescriptors[0] = recordDescriptor;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.api.dataflow.IActivity#createPushRuntime(edu.uci.ics.hyracks.api.context.IHyracksTaskContext,
     * edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider, int, int)
     */
    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            private ArrayList<ByteBuffer> buffers;

            @Override
            public void open() throws HyracksDataException {
                buffers = new ArrayList<ByteBuffer>();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                ByteBuffer buf = ctx.allocateFrame();
                System.arraycopy(buffer.array(), 0, buf.array(), 0, buffer.capacity());
                buffers.add(buf);
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {
                ArrayList<Integer> bufferRecIndex = new ArrayList<Integer>();

                int frameSize = ctx.getFrameSize();
                int tupleCountOffset = FrameHelper.getTupleCountOffset(frameSize);

                for (int i = 0; i < buffers.size(); i++) {
                    bufferRecIndex.add(0);
                }

                Random rand = new Random();

                ByteBuffer outputBuffer = ctx.allocateFrame();
                FrameTupleAppender appender = new FrameTupleAppender(frameSize);

                appender.reset(outputBuffer, true);

                writer.open();

                int pageToPick;
                ByteBuffer bufferToPick;

                while (buffers.size() > 0) {
                    pageToPick = rand.nextInt(buffers.size());
                    bufferToPick = buffers.get(pageToPick);
                    int tupleIndex = bufferRecIndex.get(pageToPick);
                    int tupleStartOffset = tupleIndex == 0 ? 0 : bufferToPick.getInt(FrameHelper
                            .getTupleCountOffset(frameSize) - 4 * tupleIndex);
                    int tupleEndOffset = bufferToPick.getInt(FrameHelper.getTupleCountOffset(frameSize) - 4
                            * (tupleIndex + 1));

                    int tupleLen = tupleEndOffset - tupleStartOffset;

                    if (!appender.append(bufferToPick.array(), tupleStartOffset, tupleLen)) {
                        FrameUtils.flushFrame(outputBuffer, writer);
                        appender.reset(outputBuffer, true);
                        if (!appender.append(bufferToPick.array(), tupleStartOffset, tupleLen)) {
                            throw new HyracksDataException("Failed to write a tuple into a frame!");
                        }
                    }
                    tupleIndex++;
                    if (tupleIndex >= bufferToPick.getInt(tupleCountOffset)) {
                        // frame is done
                        bufferRecIndex.remove(pageToPick);
                        buffers.remove(pageToPick);
                    } else {
                        bufferRecIndex.set(pageToPick, tupleIndex);
                    }
                }

                if (appender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(outputBuffer, writer);
                }

                buffers = null;
                bufferRecIndex = null;

                writer.close();
            }
        };
    }

}
