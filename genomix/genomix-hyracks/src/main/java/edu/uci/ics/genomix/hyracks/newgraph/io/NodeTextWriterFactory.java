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
package edu.uci.ics.genomix.hyracks.newgraph.io;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.hyracks.newgraph.dataflow.ReadsKeyValueParserFactory;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;

public class NodeTextWriterFactory implements ITupleWriterFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final int kmerSize;
    public static final int OutputKmerField = ReadsKeyValueParserFactory.OutputKmerField;
    public static final int outputNodeField = ReadsKeyValueParserFactory.OutputNodeField;
    
    public NodeTextWriterFactory(int k) {
        this.kmerSize = k;
    }

    @Override
    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx) throws HyracksDataException {
        KmerBytesWritable.setGlobalKmerLength(kmerSize);
        return new ITupleWriter() {
            NodeWritable node = new NodeWritable();
            
            @Override
            public void open(DataOutput output) throws HyracksDataException {

            }

            @Override
            public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
                node.setAsReference(tuple.getFieldData(outputNodeField), tuple.getFieldStart(outputNodeField));
                node.getKmer().reset(kmerSize);
                node.getKmer().setAsReference(tuple.getFieldData(OutputKmerField), tuple.getFieldStart(OutputKmerField));
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
