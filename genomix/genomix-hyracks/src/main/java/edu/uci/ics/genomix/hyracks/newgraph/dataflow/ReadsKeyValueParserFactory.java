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

package edu.uci.ics.genomix.hyracks.newgraph.dataflow;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.uci.ics.genomix.type.IntermediateNodeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParserFactory;

public class ReadsKeyValueParserFactory implements IKeyValueParserFactory<LongWritable, Text> {
    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(ReadsKeyValueParserFactory.class);

    public static final int OutputKmerField = 0;
    public static final int OutputNodeIdField = 1;
    public static final int OutputForwardForwardField = 2;
    public static final int OutputForwardReverseField = 3;
    public static final int OutputReverseForwardField = 4;
    public static final int OutputReverseReverseField = 5;

    private final int readLength;
    private final int kmerSize;

    public static final RecordDescriptor readKmerOutputRec = new RecordDescriptor(new ISerializerDeserializer[] { null,
            null });

    public ReadsKeyValueParserFactory(int readlength, int k, boolean bGenerateReversed) {
        this.readLength = readlength;
        this.kmerSize = k;
    }

    @Override
    public IKeyValueParser<LongWritable, Text> createKeyValueParser(final IHyracksTaskContext ctx) {
        final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        final ByteBuffer outputBuffer = ctx.allocateFrame();
        final FrameTupleAppender outputAppender = new FrameTupleAppender(ctx.getFrameSize());
        outputAppender.reset(outputBuffer, true);

        return new IKeyValueParser<LongWritable, Text>() {

            private KmerBytesWritable kmer = new KmerBytesWritable(kmerSize);
            private KmerBytesWritable nextKmer = new KmerBytesWritable(kmerSize);
            private PositionWritable nodeId = new PositionWritable();
            private KmerListWritable kmerList = new KmerListWritable(kmerSize);
            private IntermediateNodeWritable interMediateNode = new IntermediateNodeWritable();
            private byte mateId = 0;
            
            @Override
            public void parse(LongWritable key, Text value, IFrameWriter writer) throws HyracksDataException {
                String[] geneLine = value.toString().split("\\t"); // Read the Real Gene Line
                if (geneLine.length != 2) {
                    return;
                }
                int readID = 0;
                try {
                    readID = Integer.parseInt(geneLine[0]);
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid data ");
                    return;
                }

                Pattern genePattern = Pattern.compile("[AGCT]+");
                Matcher geneMatcher = genePattern.matcher(geneLine[1]);
                boolean isValid = geneMatcher.matches();
                if (isValid) {
                    if (geneLine[1].length() != readLength) {
                        LOG.warn("Invalid readlength at: " + readID);
                        return;
                    }
                    SplitReads(readID, geneLine[1].getBytes(), writer);
                }
            }

            private void SplitReads(int readID, byte[] array, IFrameWriter writer) {
                /** first kmer */
                if (kmerSize >= array.length) {
                    return;
                }
                kmer.setByRead(array, 0);
                nextKmer.set(kmer);
                nextKmer.shiftKmerWithNextChar(array[kmerSize]);
                kmerList.append(nextKmer);
                nodeId.set(mateId, readID, 1);
                interMediateNode.setNodeId(nodeId);
                interMediateNode.setFFList(kmerList);
                InsertToFrame(kmer, interMediateNode, writer);

                /** middle kmer */
                for (int i = kmerSize; i < array.length; i++) {
                    kmer.shiftKmerWithNextChar(array[i]);
                    nextKmer.set(kmer);
                    nextKmer.shiftKmerWithNextChar(array[i+1]);
                    kmerList.append(nextKmer);
                    nodeId.set(mateId, readID, i - kmerSize + 2);
                    interMediateNode.setNodeId(nodeId);
                    interMediateNode.setFFList(kmerList);
                    InsertToFrame(kmer, interMediateNode, writer);
                }
            }

            private void InsertToFrame(KmerBytesWritable kmer, IntermediateNodeWritable node, IFrameWriter writer) {
                try {
                    if (Math.abs(node.getNodeId().getPosId()) > 32768) {
                        throw new IllegalArgumentException("Position id is beyond 32768 at " + node.getNodeId().getReadId());
                    }
                    tupleBuilder.reset();
                    tupleBuilder.addField(kmer.getBytes(), kmer.getOffset(), kmer.getLength());
                    tupleBuilder.addField(node.getNodeId().getByteArray(), node.getNodeId().getStartOffset(), node.getNodeId().getLength());
                    tupleBuilder.addField(node.getFFList().getByteArray(), node.getFFList().getStartOffset(), node.getFFList().getLength());
                    tupleBuilder.addField(node.getFRList().getByteArray(), node.getFRList().getStartOffset(), node.getFRList().getLength());
                    tupleBuilder.addField(node.getRFList().getByteArray(), node.getRFList().getStartOffset(), node.getRFList().getLength());
                    tupleBuilder.addField(node.getRRList().getByteArray(), node.getRRList().getStartOffset(), node.getRRList().getLength());
                    
                    if (!outputAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                            tupleBuilder.getSize())) {
                        FrameUtils.flushFrame(outputBuffer, writer);
                        outputAppender.reset(outputBuffer, true);
                        if (!outputAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                tupleBuilder.getSize())) {
                            throw new IllegalStateException(
                                    "Failed to copy an record into a frame: the record kmerByteSize is too large.");
                        }
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public void open(IFrameWriter writer) throws HyracksDataException {
            }

            @Override
            public void close(IFrameWriter writer) throws HyracksDataException {
                FrameUtils.flushFrame(outputBuffer, writer);
            }
        };
    }

}
