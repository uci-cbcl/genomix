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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.JobConf;
import org.genomix.driver.GenomixJobConf;

import edu.uci.ics.genomix.hyracks.graph.dataflow.ReadsKeyValueParserFactory;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;

@SuppressWarnings("deprecation")
public class KeyValueSequenceWriterFactory implements ITupleWriterFactory {

    /**
     * Write the node to Text
     */
    private static final long serialVersionUID = 1L;
    private final int kmerSize;
    private ConfFactory confFactory;
    
    public KeyValueSequenceWriterFactory(JobConf conf) throws HyracksDataException {
        this.confFactory = new ConfFactory(conf);
        this.kmerSize = Integer.parseInt(conf.get(GenomixJobConf.KMER_LENGTH));
    }

    public class TupleWriter implements ITupleWriter {

        public TupleWriter(ConfFactory confFactory) {
            this.cf = confFactory;
        }

        ConfFactory cf;
        Writer writer = null;
        private NodeWritable outputNode = new NodeWritable();
        private KmerBytesWritable tempKmer = new KmerBytesWritable();
        private VKmerBytesWritable outputKey = new VKmerBytesWritable();

        @Override
        public void open(DataOutput output) throws HyracksDataException {
            try {
                writer = SequenceFile.createWriter(cf.getConf(), (FSDataOutputStream) output, VKmerBytesWritable.class,
                        NodeWritable.class, CompressionType.NONE, null);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
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
                writer.append(outputKey, outputNode);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void close(DataOutput output) throws HyracksDataException {
        }

    }

    @Override
    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx) throws HyracksDataException {
        KmerBytesWritable.setGlobalKmerLength(kmerSize);
        return new TupleWriter(confFactory);
    }

}
