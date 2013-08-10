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

package edu.uci.ics.genomix.hyracks.graph.dataflow;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
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
    public IKeyValueParser<LongWritable, Text> createKeyValueParser(final IHyracksTaskContext ctx) throws HyracksDataException {
        final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        final ByteBuffer outputBuffer = ctx.allocateFrame();
        final FrameTupleAppender outputAppender = new FrameTupleAppender(ctx.getFrameSize());
        outputAppender.reset(outputBuffer, true);
        KmerBytesWritable.setGlobalKmerLength(kmerSize);
        return new IKeyValueParser<LongWritable, Text>() {

            private PositionWritable readId = new PositionWritable();
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
                curNode.setAvgCoverage(1);
                nextNode.setAvgCoverage(1);
                curForwardKmer.setByRead(array, 0);
                curReverseKmer.setByReadReverse(array, 0);
                curKmerDir = curForwardKmer.compareTo(curReverseKmer) <= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
                nextForwardKmer.setAsCopy(curForwardKmer);
                nextKmerDir = setNextKmer(nextForwardKmer, nextReverseKmer, array[kmerSize]);
                setThisReadId(readId, mateId, readID, 0);
                if(curKmerDir == KmerDir.FORWARD)
                    curNode.getStartReads().append(readId);
                else
                    curNode.getEndReads().append(readId);
                setEdgeAndThreadListForCurAndNextKmer(curKmerDir, curNode, nextKmerDir, nextNode, readId);
                writeToFrame(curForwardKmer, curReverseKmer, curKmerDir, curNode, writer);
                /*middle kmer*/
                int i = kmerSize + 1;
                for (; i < array.length; i++) {
                    curForwardKmer.setAsCopy(nextForwardKmer);
                    curReverseKmer.setAsCopy(nextReverseKmer);
                    curKmerDir = nextKmerDir;
                    curNode.setAsCopy(nextNode);
                    nextNode.reset();
                    nextNode.setAvgCoverage(1);
                    nextKmerDir = setNextKmer(nextForwardKmer, nextReverseKmer, array[i]);
                    setEdgeAndThreadListForCurAndNextKmer(curKmerDir, curNode, nextKmerDir, nextNode, readId);
                    writeToFrame(curForwardKmer, curReverseKmer, curKmerDir, curNode, writer);
                }

                /*last kmer*/
                writeToFrame(nextForwardKmer, nextReverseKmer, nextKmerDir, nextNode, writer);
            }

            public void setThisReadId(PositionWritable readId, byte mateId, long readID, int posId) {
                readId.set(mateId, readID, posId);
            }

            public KmerDir setNextKmer(KmerBytesWritable forwardKmer, KmerBytesWritable ReverseKmer,
                    byte nextChar) {
                forwardKmer.shiftKmerWithNextChar(nextChar);
                ReverseKmer.setByReadReverse(forwardKmer.toString().getBytes(), forwardKmer.getOffset());
                return forwardKmer.compareTo(ReverseKmer) <= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
            }

            public void writeToFrame(KmerBytesWritable forwardKmer, KmerBytesWritable reverseKmer, KmerDir curKmerDir,
                    NodeWritable node, IFrameWriter writer) {
                switch (curKmerDir) {
                    case FORWARD:
                        InsertToFrame(forwardKmer, node, writer);
                        break;
                    case REVERSE:
                        InsertToFrame(reverseKmer, node, writer);
                        break;
                }
            }

            public void setEdgeAndThreadListForCurAndNextKmer(KmerDir curKmerDir, NodeWritable curNode, KmerDir nextKmerDir,
                    NodeWritable nextNode, PositionWritable readId) {
                if (curKmerDir == KmerDir.FORWARD && nextKmerDir == KmerDir.FORWARD) {
//                    // TODO: clean up this section!  I'm going to leave a syntax error here for you so you can take care of it...
                    hi = Nan!  Clean this section up, please!
//                    curNode.getEdgeList(DirectionFlag.DIR_FF).append(kmerSize, nextForwardKmer);
//                    curNode.getThreadList(DirectionFlag.DIR_FF).append(readId);
                    curNode.getEdgeList(DirectionFlag.DIR_FF).unionUpdate(new EdgeListWritable(Arrays.asList(new EdgeWritable(new VKmerBytesWritable(nextForwardKmer), new PositionListWritable(Arrays.asList(readId))))));
//                    nextNode.getEdgeList(DirectionFlag.DIR_RR).append(kmerSize, curForwardKmer);
//                    nextNode.getThreadList(DirectionFlag.DIR_RR).append(readId);
                    nextNode.getEdgeList(DirectionFlag.DIR_RR).unionUpdate(new EdgeListWritable(Arrays.asList(new EdgeWritable(new VKmerBytesWritable(curForwardKmer), new PositionListWritable(Arrays.asList(readId))))));
                }
                if (curKmerDir == KmerDir.FORWARD && nextKmerDir == KmerDir.REVERSE) {
//                    curNode.getEdgeList(DirectionFlag.DIR_FR).append(kmerSize, nextReverseKmer);
//                    curNode.getThreadList(DirectionFlag.DIR_FR).append(readId);
                    curNode.getEdgeList(DirectionFlag.DIR_FR).unionUpdate(new EdgeListWritable(Arrays.asList(new EdgeWritable(new VKmerBytesWritable(nextReverseKmer), new PositionListWritable(Arrays.asList(readId))))));
//                    nextNode.getEdgeList(DirectionFlag.DIR_FR).append(kmerSize, curForwardKmer);
//                    nextNode.getThreadList(DirectionFlag.DIR_FR).append(readId);
                    nextNode.getEdgeList(DirectionFlag.DIR_FR).unionUpdate(new EdgeListWritable(Arrays.asList(new EdgeWritable(new VKmerBytesWritable(curForwardKmer), new PositionListWritable(Arrays.asList(readId))))));
                }
                if (curKmerDir == KmerDir.REVERSE && nextKmerDir == KmerDir.FORWARD) {
//                    curNode.getEdgeList(DirectionFlag.DIR_RF).append(kmerSize, nextForwardKmer);
//                    curNode.getThreadList(DirectionFlag.DIR_RF).append(readId);
                    curNode.getEdgeList(DirectionFlag.DIR_RF).unionUpdate(new EdgeListWritable(Arrays.asList(new EdgeWritable(new VKmerBytesWritable(nextForwardKmer), new PositionListWritable(Arrays.asList(readId))))));
//                    nextNode.getEdgeList(DirectionFlag.DIR_RF).append(kmerSize, curReverseKmer);
//                    nextNode.getThreadList(DirectionFlag.DIR_RF).append(readId);
                    nextNode.getEdgeList(DirectionFlag.DIR_RF).unionUpdate(new EdgeListWritable(Arrays.asList(new EdgeWritable(new VKmerBytesWritable(curReverseKmer), new PositionListWritable(Arrays.asList(readId))))));
                }
                if (curKmerDir == KmerDir.REVERSE && nextKmerDir == KmerDir.REVERSE) {
//                    curNode.getEdgeList(DirectionFlag.DIR_RR).append(kmerSize, nextReverseKmer);
//                    curNode.getThreadList(DirectionFlag.DIR_RR).append(readId);
                    curNode.getEdgeList(DirectionFlag.DIR_RR).unionUpdate(new EdgeListWritable(Arrays.asList(new EdgeWritable(new VKmerBytesWritable(nextReverseKmer), new PositionListWritable(Arrays.asList(readId))))));
//                    nextNode.getEdgeList(DirectionFlag.DIR_FF).append(kmerSize, curReverseKmer);
//                    nextNode.getThreadList(DirectionFlag.DIR_FF).append(readId);
                    nextNode.getEdgeList(DirectionFlag.DIR_FF).unionUpdate(new EdgeListWritable(Arrays.asList(new EdgeWritable(new VKmerBytesWritable(curReverseKmer), new PositionListWritable(Arrays.asList(readId))))));
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
