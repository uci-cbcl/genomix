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

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
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
    public static final int OutputNodeField = 1;

    private final int readLength;
    private final int kmerSize;

    public static final RecordDescriptor readKmerOutputRec = new RecordDescriptor(new ISerializerDeserializer[] { null,
            null });

    public ReadsKeyValueParserFactory(int readlength, int k) {
        this.readLength = readlength;
        this.kmerSize = k;
    }

    public enum KmerDir {
        FORWARD,
        REVERSE,
    }

    @Override
    public IKeyValueParser<LongWritable, Text> createKeyValueParser(final IHyracksTaskContext ctx) {
        final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        final ByteBuffer outputBuffer = ctx.allocateFrame();
        final FrameTupleAppender outputAppender = new FrameTupleAppender(ctx.getFrameSize());
        outputAppender.reset(outputBuffer, true);
        KmerBytesWritable.setGlobalKmerLength(kmerSize);
        return new IKeyValueParser<LongWritable, Text>() {

            private PositionWritable nodeId = new PositionWritable();
            private PositionListWritable nodeIdList = new PositionListWritable();
            private NodeWritable curNode = new NodeWritable();
            private NodeWritable nextNode = new NodeWritable();

            private KmerBytesWritable curForwardKmer = new KmerBytesWritable();
            private KmerBytesWritable curReverseKmer = new KmerBytesWritable();
            private KmerBytesWritable nextForwardKmer = new KmerBytesWritable();
            private KmerBytesWritable nextReverseKmer = new KmerBytesWritable();

            private KmerDir curKmerDir = KmerDir.FORWARD;
            private KmerDir nextKmerDir = KmerDir.FORWARD;

            byte mateId = (byte) 0;

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
                /*first kmer*/
                if (kmerSize >= array.length) {
                    return;
                }
                curNode.reset();
                nextNode.reset();
                curForwardKmer.setByRead(array, 0);
                curReverseKmer.setByReadReverse(array, 0);
                curKmerDir = curForwardKmer.compareTo(curReverseKmer) <= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
                nextForwardKmer.setAsCopy(curForwardKmer);
                setNextKmer(nextForwardKmer, nextReverseKmer, nextKmerDir, array[kmerSize]);
                setnodeId(curNode, mateId, readID, 0);
                setnodeId(nextNode, mateId, readID, 0);
                setEdgeListForCurAndNextKmer(curKmerDir, curNode, nextKmerDir, nextNode);
                writeToFrame(curForwardKmer, curReverseKmer, curKmerDir, curNode, writer);

                /*middle kmer*/
                int i = kmerSize + 1;
                for (; i < array.length; i++) {
                    curForwardKmer.setAsCopy(nextForwardKmer);
                    curReverseKmer.setAsCopy(nextReverseKmer);
                    curNode.set(nextNode);
                    nextNode.reset();
                    setNextKmer(nextForwardKmer, nextReverseKmer, nextKmerDir, array[kmerSize]);
                    setnodeId(nextNode, mateId, readID, 0);
                    setEdgeListForCurAndNextKmer(curKmerDir, curNode, nextKmerDir, nextNode);
                    writeToFrame(curForwardKmer, curReverseKmer, curKmerDir, curNode, writer);
                }

                /*last kmer*/
                writeToFrame(nextForwardKmer, nextReverseKmer, nextKmerDir, nextNode, writer);
            }

            public void setnodeId(NodeWritable node, byte mateId, long readID, int posId) {
                nodeId.set(mateId, readID, posId);
                nodeIdList.reset();
                nodeIdList.append(nodeId);
                node.setNodeIdList(nodeIdList);
            }

            public void setNextKmer(KmerBytesWritable forwardKmer, KmerBytesWritable ReverseKmer, KmerDir nextKmerDir,
                    byte nextChar) {
                forwardKmer.shiftKmerWithNextChar(nextChar);
                ReverseKmer.setByReadReverse(forwardKmer.toString().getBytes(), forwardKmer.getOffset());
                nextKmerDir = forwardKmer.compareTo(ReverseKmer) <= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
            }

            public void writeToFrame(KmerBytesWritable forwardKmer, KmerBytesWritable reverseKmer, KmerDir curKmerDir,
                    NodeWritable node, IFrameWriter writer) {
                switch (curKmerDir) {
                    case FORWARD:
                        InsertToFrame(forwardKmer, node, writer);
                        break;
                    case REVERSE:
                        InsertToFrame(forwardKmer, node, writer);
                        break;
                }
            }

            public void setEdgeListForCurAndNextKmer(KmerDir curKmerDir, NodeWritable curNode, KmerDir nextKmerDir,
                    NodeWritable nextNode) {
                if (curKmerDir == KmerDir.FORWARD && nextKmerDir == KmerDir.FORWARD) {
                    curNode.getFFList().append(kmerSize, nextForwardKmer);
                    nextNode.getRRList().append(kmerSize, curForwardKmer);
                }
                if (curKmerDir == KmerDir.FORWARD && nextKmerDir == KmerDir.REVERSE) {
                    curNode.getFRList().append(kmerSize, nextReverseKmer);
                    nextNode.getFRList().append(kmerSize, curForwardKmer);
                }
                if (curKmerDir == KmerDir.REVERSE && nextKmerDir == KmerDir.FORWARD) {
                    curNode.getRFList().append(kmerSize, nextForwardKmer);
                    nextNode.getRFList().append(kmerSize, curReverseKmer);
                }
                if (curKmerDir == KmerDir.REVERSE && nextKmerDir == KmerDir.REVERSE) {
                    curNode.getRRList().append(kmerSize, nextReverseKmer);
                    nextNode.getFFList().append(kmerSize, curReverseKmer);
                }
            }

            private void InsertToFrame(KmerBytesWritable kmer, NodeWritable node, IFrameWriter writer) {
                try {
                    tupleBuilder.reset();
                    tupleBuilder.addField(kmer.getBytes(), kmer.getOffset(), kmer.getLength());
                    tupleBuilder.addField(node.marshalToByteArray(), 0, node.getSerializedLength());

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
