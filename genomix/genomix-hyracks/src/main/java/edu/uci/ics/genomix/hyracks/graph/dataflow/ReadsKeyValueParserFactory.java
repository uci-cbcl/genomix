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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;
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

            private ReadHeadInfo readHeadInfo = new ReadHeadInfo(0);
            private ReadIdSet readIdSet = new ReadIdSet();
            private Node curNode = new Node();
            private Node nextNode = new Node();

            private Kmer curForwardKmer = new Kmer();
            private Kmer curReverseKmer = new Kmer();
            private Kmer nextForwardKmer = new Kmer();
            private Kmer nextReverseKmer = new Kmer();

            @Override
            public void parse(LongWritable key, Text value, IFrameWriter writer, String filename)
                    throws HyracksDataException {

                String basename = filename.substring(filename.lastIndexOf(File.separator) + 1);
                String extension = basename.substring(basename.lastIndexOf('.') + 1);

                byte mateId = basename.endsWith("_2" + extension) ? (byte) 1 : (byte) 0;
                boolean fastqFormat = false;
                if (extension.contains("fastq") || extension.contains("fq")) {
                    // TODO make NLineInputFormat works on hyracks HDFS reader
                    // if (! (job.getInputFormat() instanceof NLineInputFormat))
                    // {
                    // throw new
                    // IllegalStateException("Fastq files require the NLineInputFormat (was "
                    // + job.getInputFormat() + " ).");
                    // }
                    // if (job.getInt("mapred.line.input.format.linespermap",
                    // -1) % 4 != 0) {
                    // throw new
                    // IllegalStateException("Fastq files require the `mapred.line.input.format.linespermap` option to be divisible by 4 (was "
                    // + job.get("mapred.line.input.format.linespermap") +
                    // ").");
                    // }
                    fastqFormat = true;
                }

                long readID = 0;
                String geneLine;
                if (fastqFormat) {
                    // FIXME : this is offset == readid only works on the only
                    // one input file, one solution: put the filename into the
                    // part of the readid
                    readID = key.get(); // TODO check: this is actually the
                                        // offset into the file... will it be
                                        // the same across all files?? //
                    geneLine = value.toString().trim();
                } else {
                    String[] rawLine = value.toString().split("\\t"); // Read
                                                                      // the
                                                                      // Real
                                                                      // Gene
                                                                      // Line
                    if (rawLine.length != 2) {
                        throw new HyracksDataException("invalid data");
                    }
                    readID = Long.parseLong(rawLine[0]);
                    geneLine = rawLine[1];
                }

                Pattern genePattern = Pattern.compile("[AGCT]+");
                Matcher geneMatcher = genePattern.matcher(geneLine);
                if (geneMatcher.matches()) {
                    setReadInfo(mateId, readID, 0);
                    SplitReads(readID, geneLine.getBytes(), writer);
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
                nextNode.reset();
                nextNode.setAverageCoverage(1);
                nextForwardKmer.setAsCopy(curForwardKmer);
                for (int i = Kmer.getKmerLength(); i < readLetters.length; i++) {
                    nextForwardKmer.shiftKmerWithNextChar(readLetters[i]);
                    nextReverseKmer.setReversedFromStringBytes(readLetters, i - Kmer.getKmerLength() + 1);
                    nextNodeDir = nextForwardKmer.compareTo(nextReverseKmer) <= 0 ? DIR.FORWARD : DIR.REVERSE;

                    setEdgeListForCurAndNext(curNodeDir, curNode, nextNodeDir, nextNode, readIdSet);
                    writeToFrame(curForwardKmer, curReverseKmer, curNodeDir, curNode, writer);

                    curForwardKmer.setAsCopy(nextForwardKmer);
                    curReverseKmer.setAsCopy(nextReverseKmer);
                    curNode.setAsCopy(nextNode);
                    curNodeDir = nextNodeDir;
                    nextNode.reset();
                    nextNode.setAverageCoverage(1);
                }

                /* last kmer */
                writeToFrame(curForwardKmer, curReverseKmer, curNodeDir, curNode, writer);
            }

            public void setReadInfo(byte mateId, long readId, int posId) {
                readIdSet.clear();
                readIdSet.add(readId);
                readHeadInfo.set(mateId, readId, posId);
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

            public void setEdgeListForCurAndNext(DIR curNodeDir, Node curNode, DIR nextNodeDir, Node nextNode,
                    ReadIdSet readIdList) {
                // TODO simplify this function after Anbang merge the edgeType
                // detect code
                if (curNodeDir == DIR.FORWARD && nextNodeDir == DIR.FORWARD) {
                    curNode.getEdgeMap(EDGETYPE.FF).put(new VKmer(nextForwardKmer), readIdList);
                    nextNode.getEdgeMap(EDGETYPE.RR).put(new VKmer(curForwardKmer), readIdList);
                    return;
                }
                if (curNodeDir == DIR.FORWARD && nextNodeDir == DIR.REVERSE) {
                    curNode.getEdgeMap(EDGETYPE.FR).put(new VKmer(nextReverseKmer), readIdList);
                    nextNode.getEdgeMap(EDGETYPE.FR).put(new VKmer(curForwardKmer), readIdList);
                    return;
                }
                if (curNodeDir == DIR.REVERSE && nextNodeDir == DIR.FORWARD) {
                    curNode.getEdgeMap(EDGETYPE.RF).put(new VKmer(nextForwardKmer), readIdList);
                    nextNode.getEdgeMap(EDGETYPE.RF).put(new VKmer(curReverseKmer), readIdList);
                    return;
                }
                if (curNodeDir == DIR.REVERSE && nextNodeDir == DIR.REVERSE) {
                    curNode.getEdgeMap(EDGETYPE.RR).put(new VKmer(nextReverseKmer), readIdList);
                    nextNode.getEdgeMap(EDGETYPE.FF).put(new VKmer(curReverseKmer), readIdList);
                    return;
                }
            }

            private void InsertToFrame(Kmer kmer, Node node, IFrameWriter writer) {
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
