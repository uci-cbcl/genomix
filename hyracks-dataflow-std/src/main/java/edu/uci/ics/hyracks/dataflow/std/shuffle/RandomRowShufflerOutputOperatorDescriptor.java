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
package edu.uci.ics.hyracks.dataflow.std.shuffle;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class RandomRowShufflerOutputOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    public RandomRowShufflerOutputOperatorDescriptor(JobSpecification spec) {
        super(spec, 1, 1);
        recordDescriptors[0] = new RecordDescriptor(
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });
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
        final FrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), new RecordDescriptor(
                new ISerializerDeserializer[] { Integer64SerializerDeserializer.INSTANCE,
                        UTF8StringSerializerDeserializer.INSTANCE }));

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            private ByteBuffer outputBuffer;

            private FrameTupleAppender outputAppender;

            private ArrayTupleBuilder tupleBuilder;

            @Override
            public void open() throws HyracksDataException {
                outputBuffer = ctx.allocateFrame();
                tupleBuilder = new ArrayTupleBuilder(1);
                outputAppender = new FrameTupleAppender(ctx.getFrameSize());
                outputAppender.reset(outputBuffer, true);
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tupleBuilder.reset();
                    tupleBuilder.addField(accessor, i, 1);
                    outputAppender.reset(outputBuffer, false);
                    if (!outputAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                            tupleBuilder.getSize())) {
                        FrameUtils.flushFrame(outputBuffer, writer);
                        outputAppender.reset(outputBuffer, true);
                        if (!outputAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                tupleBuilder.getSize())) {
                            throw new HyracksDataException(
                                    "Failed to copy an record into a frame: the record size is too large.");
                        }
                    }
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {
                if (outputAppender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(outputBuffer, writer);
                }
                outputAppender = null;
                outputBuffer = null;
                writer.close();
            }
        };
    }
}
