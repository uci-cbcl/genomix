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

import edu.uci.ics.genomix.oldtype.PositionListWritable;
import edu.uci.ics.genomix.oldtype.PositionWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;

public class KMerTextWriterFactory implements ITupleWriterFactory {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    private final int kmerSize;

    public KMerTextWriterFactory(int k) {
        kmerSize = k;
    }

    public class TupleWriter implements ITupleWriter {
        private KmerBytesWritable kmer = new KmerBytesWritable(kmerSize);
        private PositionListWritable plist = new PositionListWritable();

        @Override
        public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
            try {
                if (kmer.getLength() > tuple.getFieldLength(KMerSequenceWriterFactory.InputKmerField)) {
                    throw new IllegalArgumentException("Not enough kmer bytes");
                }
                kmer.setNewReference(tuple.getFieldData(KMerSequenceWriterFactory.InputKmerField),
                        tuple.getFieldStart(KMerSequenceWriterFactory.InputKmerField));
                int countOfPos = tuple.getFieldLength(KMerSequenceWriterFactory.InputPositionListField)
                        / PositionWritable.LENGTH;
                if (tuple.getFieldLength(KMerSequenceWriterFactory.InputPositionListField) % PositionWritable.LENGTH != 0) {
                    throw new IllegalArgumentException("Invalid count of position byte");
                }
                plist.setNewReference(countOfPos, tuple.getFieldData(KMerSequenceWriterFactory.InputPositionListField),
                        tuple.getFieldStart(KMerSequenceWriterFactory.InputPositionListField));

                output.write(kmer.toString().getBytes());
                output.writeByte('\t');
                output.write(plist.toString().getBytes());
                output.writeByte('\n');
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void open(DataOutput output) throws HyracksDataException {

        }

        @Override
        public void close(DataOutput output) throws HyracksDataException {
        }
    }

    @Override
    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx) throws HyracksDataException {
        return new TupleWriter();
    }

}
