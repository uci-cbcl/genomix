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
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;

public class NodeTextWriterFactory implements ITupleWriterFactory {

    /**
     * Write the node to Text
     */
    private static final long serialVersionUID = 1L;
    private final int kmerSize;
    public static final int OutputNodeField = AssembleKeyIntoNodeOperator.OutputNodeField;

    public NodeTextWriterFactory(int k) {
        this.kmerSize = k;
    }

    @Override
    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx, int partition, int nPartition)
            throws HyracksDataException {
        Kmer.setGlobalKmerLength(kmerSize);
        return new ITupleWriter() {
            Node node = new Node();

            @Override
            public void open(DataOutput output) throws HyracksDataException {

            }

            @Override
            public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
                node.setAsReference(tuple.getFieldData(OutputNodeField), tuple.getFieldStart(OutputNodeField));
                try {
                    output.write(node.toString().getBytes());
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
