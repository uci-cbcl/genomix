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

package edu.uci.ics.genomix.hyracks.dataflow.io;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.genomix.hyracks.job.GenomixJobConf;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;

@SuppressWarnings("deprecation")
public class KMerSequenceWriterFactory implements ITupleWriterFactory {

    private static final long serialVersionUID = 1L;
    private ConfFactory confFactory;
    private final int kmerlength;

    public static final int InputKmerField = 0;
    public static final int InputPositionListField = 1;

    public KMerSequenceWriterFactory(JobConf conf) throws HyracksDataException {
        this.confFactory = new ConfFactory(conf);
        this.kmerlength = conf.getInt(GenomixJobConf.KMER_LENGTH, GenomixJobConf.DEFAULT_KMERLEN);
    }

    public class TupleWriter implements ITupleWriter {
        public TupleWriter(ConfFactory cf) {
            this.cf = cf;
        }

        ConfFactory cf;
        Writer writer = null;

        KmerBytesWritable reEnterKey = new KmerBytesWritable(kmerlength);
        PositionListWritable plist = new PositionListWritable();

        /**
         * assumption is that output never change source!
         */
        @Override
        public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
            try {
                if (reEnterKey.getLength() > tuple.getFieldLength(InputKmerField)) {
                    throw new IllegalArgumentException("Not enough kmer bytes");
                }
                reEnterKey.setNewReference(tuple.getFieldData(InputKmerField), tuple.getFieldStart(InputKmerField));
                int countOfPos = tuple.getFieldLength(InputPositionListField) / PositionWritable.LENGTH;
                if (tuple.getFieldLength(InputPositionListField) % PositionWritable.LENGTH != 0) {
                    throw new IllegalArgumentException("Invalid count of position byte");
                }
                plist.setNewReference(countOfPos, tuple.getFieldData(InputPositionListField),
                        tuple.getFieldStart(InputPositionListField));

                writer.append(reEnterKey, plist);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void open(DataOutput output) throws HyracksDataException {
            try {
                writer = SequenceFile.createWriter(cf.getConf(), (FSDataOutputStream) output, KmerBytesWritable.class,
                        PositionListWritable.class, CompressionType.NONE, null);
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
        return new TupleWriter(confFactory);
    }

}
