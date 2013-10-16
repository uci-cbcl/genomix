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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;

public class KmerNodePairSequenceWriterFactory implements ITupleWriterFactory {

    private static final long serialVersionUID = 1L;
    private ConfFactory confFactory;

    public KmerNodePairSequenceWriterFactory(JobConf conf) throws HyracksDataException {
        this.confFactory = new ConfFactory(conf);
        Kmer.setGlobalKmerLength(Integer.parseInt(conf.get(GenomixJobConf.KMER_LENGTH)));
    }

    public class TupleWriter implements ITupleWriter {

        public TupleWriter(ConfFactory confFactory) {
            this.cf = confFactory;
        }

        ConfFactory cf;
        Writer writer = null;

        private Node outputNode = new Node();
        private Kmer cachedKmer = new Kmer();
        private VKmer outputVKmer = new VKmer(Kmer.getKmerLength());

        @Override
        public void open(DataOutput output) throws HyracksDataException {
            try {
                writer = SequenceFile.createWriter(cf.getConf(), (FSDataOutputStream) output, VKmer.class, Node.class,
                        CompressionType.NONE, null);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        /**
         * We need to output VKmer as a key, but the coming data is Kmer, we need to get the Kmer first and then set and output the VKmer
         */
        @Override
        public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
            try {
                if (cachedKmer.getLength() > tuple.getFieldLength(ReadsKeyValueParserFactory.OutputKmerField)) {
                    throw new IllegalArgumentException("Not enough kmer bytes");
                }

                cachedKmer.setAsReference(tuple.getFieldData(ReadsKeyValueParserFactory.OutputKmerField),
                        tuple.getFieldStart(ReadsKeyValueParserFactory.OutputKmerField));
                outputVKmer.setAsCopy(cachedKmer);
                outputNode.setAsReference(tuple.getFieldData(ReadsKeyValueParserFactory.OutputNodeField),
                        tuple.getFieldStart(ReadsKeyValueParserFactory.OutputNodeField));
                writer.append(outputVKmer, outputNode);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void close(DataOutput output) throws HyracksDataException {
        }

    }

    @Override
    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx, int partition, int nPartition)
            throws HyracksDataException {
        return new TupleWriter(confFactory);
    }

}
