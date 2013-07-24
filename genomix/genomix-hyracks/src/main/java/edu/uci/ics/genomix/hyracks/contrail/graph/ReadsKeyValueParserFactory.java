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

package edu.uci.ics.genomix.hyracks.contrail.graph;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.uci.ics.genomix.hyracks.data.primitive.PositionReference;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.IntermediateNodeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerBytesWritableFactory;
import edu.uci.ics.genomix.type.KmerListWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.ReadIDWritable;
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
    public static final int OutputPosition = 1;

    private final boolean bReversed;
    private final int readLength;
    private final int kmerSize;

    public static final RecordDescriptor readKmerOutputRec = new RecordDescriptor(new ISerializerDeserializer[] { null,
            null, null, null, null, null, null});

    public ReadsKeyValueParserFactory(int readlength, int k, boolean bGenerateReversed) {
        bReversed = bGenerateReversed;
        this.readLength = readlength;
        this.kmerSize = k;
    }

    public static enum KmerDir {
        FORWARD,
        REVERSE,
    }

    @Override
    public IKeyValueParser<LongWritable, Text> createKeyValueParser(final IHyracksTaskContext ctx) {
        final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        final ByteBuffer outputBuffer = ctx.allocateFrame();
        final FrameTupleAppender outputAppender = new FrameTupleAppender(ctx.getFrameSize());
        outputAppender.reset(outputBuffer, true);

        return new IKeyValueParser<LongWritable, Text>() {

            private KmerBytesWritable kmer = new KmerBytesWritable(kmerSize);//
            private PositionReference pos = new PositionReference();//

            private KmerBytesWritable preForwardKmer = new KmerBytesWritable(kmerSize);
            private KmerBytesWritable preReverseKmer = new KmerBytesWritable(kmerSize);
            private KmerBytesWritable curForwardKmer = new KmerBytesWritable(kmerSize);
            private KmerBytesWritable curReverseKmer = new KmerBytesWritable(kmerSize);
            private KmerBytesWritable nextForwardKmer = new KmerBytesWritable(kmerSize);
            private KmerBytesWritable nextReverseKmer = new KmerBytesWritable(kmerSize);
            private IntermediateNodeWritable outputNode = new IntermediateNodeWritable();
            private ReadIDWritable readId = new ReadIDWritable();
            private KmerListWritable kmerList = new KmerListWritable();

            private KmerBytesWritableFactory kmerFactory = new KmerBytesWritableFactory(kmerSize);
            private KmerDir preKmerDir = KmerDir.FORWARD;
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
/*                
                if (kmerSize >= array.length) {
                    return;
                }
                kmer.setByRead(array, 0);
                InsertToFrame(kmer, readID, 1, writer);

                
                for (int i = kmerSize; i < array.length; i++) {
                    kmer.shiftKmerWithNextChar(array[i]);
                    InsertToFrame(kmer, readID, i - kmerSize + 2, writer);
                }

                if (bReversed) {
                    kmer.setByReadReverse(array, 0);
                    InsertToFrame(kmer, readID, -1, writer);
                    for (int i = kmerSize; i < array.length; i++) {
                        kmer.shiftKmerWithPreCode(GeneCode.getPairedCodeFromSymbol(array[i]));
                        InsertToFrame(kmer, readID, -(i - kmerSize + 2), writer);
                    }
                }*/
                ///////////////////////////////////////
                if (kmerSize >= array.length) {
                    return;
                }
                /** first kmer **/
                curForwardKmer.setByRead(array, 0);
                curReverseKmer.set(kmerFactory.reverse(curForwardKmer));
                curKmerDir = curForwardKmer.compareTo(curReverseKmer) >= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
                setNextKmer(array[kmerSize]);
                readId.set(mateId, readID);
                outputNode.setreadId(readId);
                setEdgeListForNextKmer();
                switch (curKmerDir) {
                    case FORWARD:
                        InsertToFrame(curForwardKmer, outputNode, writer);
                        break;
                    case REVERSE:
                        InsertToFrame(curReverseKmer, outputNode, writer);
                        break;
                }
                /** middle kmer **/
                for (int i = kmerSize + 1; i < array.length; i++) {
                    setPreKmerByOldCurKmer();
                    setCurKmerByOldNextKmer();
                    setNextKmer(array[i]);
                    //set value.readId
                    readId.set(mateId, readID);
                    outputNode.setreadId(readId);
                    //set value.edgeList
                    setEdgeListForPreKmer();
                    setEdgeListForNextKmer();
                    //output mapper result
                    switch (curKmerDir) {
                        case FORWARD:
                            InsertToFrame(curForwardKmer, outputNode, writer);
                            break;
                        case REVERSE:
                            InsertToFrame(curReverseKmer, outputNode, writer);
                            break;
                    }
                }
                /** last kmer **/
                setPreKmerByOldCurKmer();
                setCurKmerByOldNextKmer();
                //set value.readId
                readId.set(mateId, readID);
                outputNode.setreadId(readId);
                //set value.edgeList
                setEdgeListForPreKmer();
                //output mapper result
                switch (curKmerDir) {
                    case FORWARD:
                        InsertToFrame(curForwardKmer, outputNode, writer);
                        break;
                    case REVERSE:
                        InsertToFrame(curReverseKmer, outputNode, writer);
                        break;
                }
            }

            public void setPreKmer(byte preChar){
                preForwardKmer.set(curForwardKmer);
                preForwardKmer.shiftKmerWithPreChar(preChar);
                preReverseKmer.set(preForwardKmer);
                preReverseKmer.set(kmerFactory.reverse(nextForwardKmer));
                preKmerDir = preForwardKmer.compareTo(preReverseKmer) >= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
            }
            
            public void setNextKmer(byte nextChar) {
                nextForwardKmer.set(curForwardKmer);
                nextForwardKmer.shiftKmerWithNextChar(nextChar);
                nextReverseKmer.set(nextForwardKmer);
                nextReverseKmer.set(kmerFactory.reverse(nextForwardKmer));
                nextKmerDir = nextForwardKmer.compareTo(nextReverseKmer) >= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
            }

            public void setPreKmerByOldCurKmer() {
                preKmerDir = curKmerDir;
                preForwardKmer.set(curForwardKmer);
                preReverseKmer.set(curReverseKmer);
            }

            //old nextKmer becomes current curKmer
            public void setCurKmerByOldNextKmer() {
                curKmerDir = nextKmerDir;
                curForwardKmer.set(nextForwardKmer);
                preReverseKmer.set(nextReverseKmer);
            }

            public void setEdgeListForNextKmer() {
                switch (curKmerDir) {
                    case FORWARD:
                        switch (nextKmerDir) {
                            case FORWARD:
                                kmerList.reset();
                                kmerList.append(nextForwardKmer);
                                outputNode.setFFList(kmerList);
                                break;
                            case REVERSE:
                                kmerList.reset();
                                kmerList.append(nextReverseKmer);
                                outputNode.setFRList(kmerList);
                                break;
                        }
                        break;
                    case REVERSE:
                        switch (nextKmerDir) {
                            case FORWARD:
                                kmerList.reset();
                                kmerList.append(nextForwardKmer);
                                outputNode.setRFList(kmerList);
                                break;
                            case REVERSE:
                                kmerList.reset();
                                kmerList.append(nextReverseKmer);
                                outputNode.setRRList(kmerList);
                                break;
                        }
                        break;
                }
            }

            public void setEdgeListForPreKmer() {
                switch (curKmerDir) {
                    case FORWARD:
                        switch (preKmerDir) {
                            case FORWARD:
                                kmerList.reset();
                                kmerList.append(preForwardKmer);
                                outputNode.setRRList(kmerList);
                                break;
                            case REVERSE:
                                kmerList.reset();
                                kmerList.append(preReverseKmer);
                                outputNode.setRFList(kmerList);
                                break;
                        }
                        break;
                    case REVERSE:
                        switch (preKmerDir) {
                            case FORWARD:
                                kmerList.reset();
                                kmerList.append(nextForwardKmer);
                                outputNode.setFRList(kmerList);
                                break;
                            case REVERSE:
                                kmerList.reset();
                                kmerList.append(nextReverseKmer);
                                outputNode.setFFList(kmerList);
                                break;
                        }
                        break;
                }
            }

            private void InsertToFrame(KmerBytesWritable kmer, IntermediateNodeWritable Node, IFrameWriter writer) {
                try {
                    tupleBuilder.reset();
                    tupleBuilder.addField(kmer.getBytes(), kmer.getOffset(), kmer.getLength());
                    
                    tupleBuilder.addField(Node.getreadId().getByteArray(), Node.getreadId().getStartOffset(), Node.getreadId().getLength());
                    
                    tupleBuilder.addField(Node.getFFList().getByteArray(), Node.getFFList().getStartOffset(), Node.getFFList()
                            .getLength());
                    tupleBuilder.addField(Node.getFRList().getByteArray(), Node.getFRList().getStartOffset(), Node.getFRList()
                            .getLength());
                    tupleBuilder.addField(Node.getRFList().getByteArray(), Node.getRFList().getStartOffset(), Node.getRFList()
                            .getLength());
                    tupleBuilder.addField(Node.getRRList().getByteArray(), Node.getRRList().getStartOffset(), Node.getRRList()
                            .getLength());

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
