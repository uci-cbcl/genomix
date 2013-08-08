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
package edu.uci.ics.genomix.hyracks.graph.io;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.hyracks.graph.dataflow.AssembleKeyIntoNodeOperator;
import edu.uci.ics.genomix.hyracks.graph.dataflow.ReadsKeyValueParserFactory;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;

public class KeyValueTextWriterFactory implements ITupleWriterFactory {

    /**
     * Write the node to Text
     */
    private static final long serialVersionUID = 1L;
    private final int kmerSize;

    public KeyValueTextWriterFactory(int k) {
        this.kmerSize = k;
    }

    @Override
    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx) throws HyracksDataException {
        KmerBytesWritable.setGlobalKmerLength(kmerSize);
        return new ITupleWriter() {
            private NodeWritable outputNode = new NodeWritable();
            private KmerBytesWritable tempKmer = new KmerBytesWritable();
            private VKmerBytesWritable outputKey = new VKmerBytesWritable();

            @Override
            public void open(DataOutput output) throws HyracksDataException {

            }

            @Override
            public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
                try {
                    if (tempKmer.getLength() > tuple.getFieldLength(ReadsKeyValueParserFactory.OutputKmerField)) {
                        throw new IllegalArgumentException("Not enough kmer bytes");
                    }
                    tempKmer.setAsReference(tuple.getFieldData(ReadsKeyValueParserFactory.OutputKmerField),
                            tuple.getFieldStart(ReadsKeyValueParserFactory.OutputKmerField));
                    outputNode.setAsReference(tuple.getFieldData(ReadsKeyValueParserFactory.OutputNodeField),
                            tuple.getFieldStart(ReadsKeyValueParserFactory.OutputNodeField));
                    outputKey.setAsCopy(tempKmer);
                    output.write(outputKey.toString().getBytes());
                    output.writeByte('\t');
                    output.write(outputNode.toString().getBytes());
                    output.writeByte('\n');
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void close(DataOutput output) throws HyracksDataException {

            }

        };
    }
}
