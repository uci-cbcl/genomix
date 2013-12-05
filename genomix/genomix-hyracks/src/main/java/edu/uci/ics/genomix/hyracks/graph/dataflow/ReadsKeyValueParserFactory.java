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
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.ReadHeadInfo;
import edu.uci.ics.genomix.data.types.VKmer;
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
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;

public class ReadsKeyValueParserFactory implements IKeyValueParserFactory<LongWritable, Text> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(ReadsKeyValueParserFactory.class.getName());

    public static final int OutputKmerField = 0;
    public static final int OutputNodeField = 1;
    private final ConfFactory confFactory;

    public static final RecordDescriptor readKmerOutputRec = new RecordDescriptor(new ISerializerDeserializer[2]);

    private static final Pattern genePattern = Pattern.compile("[AGCT]+");

    public ReadsKeyValueParserFactory(JobConf conf) throws HyracksDataException {
        confFactory = new ConfFactory(conf);
    }

    @Override
    public IKeyValueParser<LongWritable, Text> createKeyValueParser(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        final ByteBuffer outputBuffer = ctx.allocateFrame();
        final FrameTupleAppender outputAppender = new FrameTupleAppender(ctx.getFrameSize());
        outputAppender.reset(outputBuffer, true);

        GenomixJobConf.setGlobalStaticConstants(confFactory.getConf());

        return new IKeyValueParser<LongWritable, Text>() {

            private ReadHeadInfo readHeadInfo = new ReadHeadInfo();
            private Node curNode = new Node();
            private Node nextNode = new Node();

            private Kmer curForwardKmer = new Kmer();
            private Kmer curReverseKmer = new Kmer();
            private Kmer nextForwardKmer = new Kmer();
            private Kmer nextReverseKmer = new Kmer();

            private VKmer thisReadSequence = new VKmer();
            private VKmer mateReadSequence = new VKmer();

            @Override
            public void parse(LongWritable key, Text value, IFrameWriter writer, String filename) {
                long readID = 0;
                String mate0GeneLine = null;
                String mate1GeneLine = null;
                String[] rawLine = value.toString().split("\\t");
                if (rawLine.length == 2) {
                    readID = Long.parseLong(rawLine[0]);
                    mate0GeneLine = rawLine[1];
                } else if (rawLine.length == 3) {
                    readID = Long.parseLong(rawLine[0]);
                    mate0GeneLine = rawLine[1];
                    mate1GeneLine = rawLine[2];
                } else {
                    throw new IllegalStateException(
                            "input format is not true! only support id'\t'readSeq'\t'mateReadSeq or id'\t'readSeq'");
                }

                Pattern genePattern = Pattern.compile("[AGCT]+");
                if (mate0GeneLine != null) {
                    Matcher geneMatcher = genePattern.matcher(mate0GeneLine);
                    if (geneMatcher.matches()) {
                        thisReadSequence.setAsCopy(mate0GeneLine);
                        if (mate1GeneLine != null) {
                            mateReadSequence.setAsCopy(mate1GeneLine);
                            readHeadInfo.set((byte) 0, readID, 0, thisReadSequence, mateReadSequence);
                        } else {
                            readHeadInfo.set((byte) 0, readID, 0, thisReadSequence, null);
                        }
                        SplitReads(readID, mate0GeneLine.getBytes(), writer);
                    }
                }
                if (mate1GeneLine != null) {
                    Matcher geneMatcher = genePattern.matcher(mate1GeneLine);
                    if (geneMatcher.matches()) {
                        thisReadSequence.setAsCopy(mate1GeneLine);
                        mateReadSequence.setAsCopy(mate0GeneLine);
                        readHeadInfo.set((byte) 1, readID, 0, thisReadSequence, mateReadSequence);
                        SplitReads(readID, mate1GeneLine.getBytes(), writer);
                    }
                }
            }

            private void SplitReads(long readID, byte[] readLetters, IFrameWriter writer) {
                /* first kmer */
                if (Kmer.getKmerLength() >= readLetters.length) {
                    throw new IllegalArgumentException("kmersize (k=" + Kmer.getKmerLength()
                            + ") is larger than the read length (" + readLetters.length + ")");
                }

                curNode.reset();
                curNode.setAverageCoverage(1);
                curForwardKmer.setFromStringBytes(readLetters, 0);

                curReverseKmer.setReversedFromStringBytes(readLetters, 0);

                DIR curNodeDir = curForwardKmer.compareTo(curReverseKmer) <= 0 ? DIR.FORWARD : DIR.REVERSE;

                if (curNodeDir == DIR.FORWARD) {
                    curNode.getUnflippedReadIds().add(readHeadInfo);
                } else {
                    curNode.getFlippedReadIds().add(readHeadInfo);
                }

                DIR nextNodeDir = DIR.FORWARD;

                /* middle kmer */
                nextNode = new Node();
                nextNode.setAverageCoverage(1);
                nextForwardKmer.setAsCopy(curForwardKmer);
                for (int i = Kmer.getKmerLength(); i < readLetters.length; i++) {
                    nextForwardKmer.shiftKmerWithNextChar(readLetters[i]);
                    nextReverseKmer.setReversedFromStringBytes(readLetters, i - Kmer.getKmerLength() + 1);
                    nextNodeDir = nextForwardKmer.compareTo(nextReverseKmer) <= 0 ? DIR.FORWARD : DIR.REVERSE;

                    setEdgesForCurAndNext(curNodeDir, curNode, nextNodeDir, nextNode);
                    writeToFrame(curForwardKmer, curReverseKmer, curNodeDir, curNode, writer);

                    curForwardKmer.setAsCopy(nextForwardKmer);
                    curReverseKmer.setAsCopy(nextReverseKmer);
                    curNode = nextNode;
                    curNodeDir = nextNodeDir;
                    nextNode = new Node();
                    nextNode.setAverageCoverage(1);
                }

                /* last kmer */
                writeToFrame(curForwardKmer, curReverseKmer, curNodeDir, curNode, writer);
            }

            public void writeToFrame(Kmer forwardKmer, Kmer reverseKmer, DIR curNodeDir, Node node, IFrameWriter writer) {
                switch (curNodeDir) {
                    case FORWARD:
                        InsertToFrame(forwardKmer, node, writer);
                        break;
                    case REVERSE:
                        InsertToFrame(reverseKmer, node, writer);
                        break;
                }
            }

            public void setEdgesForCurAndNext(DIR curNodeDir, Node curNode, DIR nextNodeDir, Node nextNode) {
                // TODO simplify this function after Anbang merge the edgeType
                // detect code
                if (curNodeDir == DIR.FORWARD && nextNodeDir == DIR.FORWARD) {
                    curNode.getEdges(EDGETYPE.FF).append(new VKmer(nextForwardKmer));
                    nextNode.getEdges(EDGETYPE.RR).append(new VKmer(curForwardKmer));

                    return;
                }
                if (curNodeDir == DIR.FORWARD && nextNodeDir == DIR.REVERSE) {
                    curNode.getEdges(EDGETYPE.FR).append(new VKmer(nextReverseKmer));
                    nextNode.getEdges(EDGETYPE.FR).append(new VKmer(curForwardKmer));
                    return;
                }
                if (curNodeDir == DIR.REVERSE && nextNodeDir == DIR.FORWARD) {
                    curNode.getEdges(EDGETYPE.RF).append(new VKmer(nextForwardKmer));
                    nextNode.getEdges(EDGETYPE.RF).append(new VKmer(curReverseKmer));
                    return;
                }
                if (curNodeDir == DIR.REVERSE && nextNodeDir == DIR.REVERSE) {
                    curNode.getEdges(EDGETYPE.RR).append(new VKmer(nextReverseKmer));
                    nextNode.getEdges(EDGETYPE.FF).append(new VKmer(curReverseKmer));
                    return;
                }
            }

            private void InsertToFrame(Kmer kmer, Node node, IFrameWriter writer) {
                try {
                    tupleBuilder.reset();
                    tupleBuilder.addField(kmer.getBytes(), kmer.getOffset(), kmer.getLength());
                    byte[] nodeBytes = node.marshalToByteArray();
                    tupleBuilder.addField(nodeBytes, 0, nodeBytes.length);

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
