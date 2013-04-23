/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.genomix.util;

import java.nio.ByteBuffer;

import edu.uci.ics.genomix.data.std.accessors.ByteSerializerDeserializer;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerCountValue;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParserFactory;

public class StatReadsKeyValueParserFactory implements IKeyValueParserFactory<KmerBytesWritable, KmerCountValue> {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    @Override
    public IKeyValueParser<KmerBytesWritable, KmerCountValue> createKeyValueParser(IHyracksTaskContext ctx)
            throws HyracksDataException {

        final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        final ByteBuffer outputBuffer = ctx.allocateFrame();
        final FrameTupleAppender outputAppender = new FrameTupleAppender(ctx.getFrameSize());
        outputAppender.reset(outputBuffer, true);

        return new IKeyValueParser<KmerBytesWritable, KmerCountValue>() {

            @Override
            public void open(IFrameWriter writer) throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void parse(KmerBytesWritable key, KmerCountValue value, IFrameWriter writer)
                    throws HyracksDataException {
                byte adjMap = value.getAdjBitMap();
                byte count = value.getCount();
                InsertToFrame((byte) (GeneCode.inDegree(adjMap)), (byte) (GeneCode.outDegree(adjMap)), count, writer);
            }

            @Override
            public void close(IFrameWriter writer) throws HyracksDataException {
                FrameUtils.flushFrame(outputBuffer, writer);
            }

            private void InsertToFrame(byte indegree, byte outdegree, byte count, IFrameWriter writer) {
                try {
                    tupleBuilder.reset();
                    tupleBuilder.addField(ByteSerializerDeserializer.INSTANCE, indegree);
                    tupleBuilder.addField(ByteSerializerDeserializer.INSTANCE, outdegree);
                    tupleBuilder.addField(ByteSerializerDeserializer.INSTANCE, count);

                    if (!outputAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                            tupleBuilder.getSize())) {
                        FrameUtils.flushFrame(outputBuffer, writer);
                        outputAppender.reset(outputBuffer, true);
                        if (!outputAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                tupleBuilder.getSize())) {
                            throw new IllegalStateException(
                                    "Failed to copy an record into a frame: the record size is too large.");
                        }
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }

}
