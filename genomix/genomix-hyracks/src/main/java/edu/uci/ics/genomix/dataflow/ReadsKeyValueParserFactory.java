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

package edu.uci.ics.genomix.dataflow;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.uci.ics.genomix.data.std.accessors.ByteSerializerDeserializer;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParserFactory;

;

public class ReadsKeyValueParserFactory implements IKeyValueParserFactory<LongWritable, Text> {
    private static final long serialVersionUID = 1L;

    private KmerBytesWritable kmer;
    private boolean bReversed;

    public ReadsKeyValueParserFactory(int k, boolean bGenerateReversed) {
        bReversed = bGenerateReversed;
        kmer = new KmerBytesWritable(k);
    }

    @Override
    public IKeyValueParser<LongWritable, Text> createKeyValueParser(final IHyracksTaskContext ctx) {
        final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        final ByteBuffer outputBuffer = ctx.allocateFrame();
        final FrameTupleAppender outputAppender = new FrameTupleAppender(ctx.getFrameSize());
        outputAppender.reset(outputBuffer, true);

        return new IKeyValueParser<LongWritable, Text>() {

            @Override
            public void parse(LongWritable key, Text value, IFrameWriter writer) throws HyracksDataException {
                String geneLine = value.toString(); // Read the Real Gene Line
                Pattern genePattern = Pattern.compile("[AGCT]+");
                Matcher geneMatcher = genePattern.matcher(geneLine);
                boolean isValid = geneMatcher.matches();
                if (isValid) {
                    SplitReads(geneLine.getBytes(), writer);
                }
            }

            private void SplitReads(byte[] array, IFrameWriter writer) {
                /** first kmer */
                int k = kmer.getKmerLength();
                if (k >= array.length){
                	return;
                }
                kmer.setByRead(array, 0);
                byte pre = 0;
                byte next = GeneCode.getAdjBit(array[k]);
                InsertToFrame(kmer, pre, next, writer);

                /** middle kmer */
                for (int i = k; i < array.length - 1; i++) {
                    pre = GeneCode.getBitMapFromGeneCode(kmer.shiftKmerWithNextChar(array[i]));
                    next = GeneCode.getAdjBit(array[i + 1]);
                    InsertToFrame(kmer, pre, next, writer);
                }

                /** last kmer */
                pre = GeneCode.getBitMapFromGeneCode(kmer.shiftKmerWithNextChar(array[array.length - 1]));
                next = 0;
                InsertToFrame(kmer, pre, next, writer);

                if (bReversed) {
                    /** first kmer */
                    kmer.setByReadReverse(array, 0);
                    next = 0;
                    pre = GeneCode.getAdjBit(array[k]);
                    InsertToFrame(kmer, pre, next, writer);
                    /** middle kmer */
                    for (int i = k; i < array.length - 1; i++) {
                        next = GeneCode.getBitMapFromGeneCode(kmer.shiftKmerWithPreChar(array[i]));
                        pre = GeneCode.getAdjBit(array[i + 1]);
                        InsertToFrame(kmer, pre, next, writer);
                    }
                    /** last kmer */
                    next = GeneCode.getBitMapFromGeneCode(kmer.shiftKmerWithPreChar(array[array.length - 1]));
                    pre = 0;
                    InsertToFrame(kmer, pre, next, writer);
                }
            }

            /**
             * At this graph building phase, we assume the kmer length are all
             * the same Thus we didn't output those Kmer length
             * 
             * @param kmer
             *            :input kmer
             * @param pre
             *            : pre neighbor code
             * @param next
             *            : next neighbor code
             * @param writer
             *            : output writer
             */
            private void InsertToFrame(KmerBytesWritable kmer, byte pre, byte next, IFrameWriter writer) {
                try {
                    byte adj = GeneCode.mergePreNextAdj(pre, next);
                    tupleBuilder.reset();
                    tupleBuilder.addField(kmer.getBytes(), 0, kmer.getLength());
                    tupleBuilder.addField(ByteSerializerDeserializer.INSTANCE, adj);

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

            @Override
            public void open(IFrameWriter writer) throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close(IFrameWriter writer) throws HyracksDataException {
                FrameUtils.flushFrame(outputBuffer, writer);
            }
        };
    }

}