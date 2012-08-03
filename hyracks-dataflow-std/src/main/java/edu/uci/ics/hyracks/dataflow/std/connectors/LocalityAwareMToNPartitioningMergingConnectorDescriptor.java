/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.connectors;

import java.util.BitSet;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractMToNConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.collectors.IPartitionBatchManager;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicPartitionBatchManager;
import edu.uci.ics.hyracks.dataflow.std.collectors.PartitionCollector;
import edu.uci.ics.hyracks.dataflow.std.collectors.SortMergeFrameReader;

public class LocalityAwareMToNPartitioningMergingConnectorDescriptor extends AbstractMToNConnectorDescriptor {

    private static final long serialVersionUID = 1L;

    private ILocalityMap localityMap;
    private ITuplePartitionComputerFactory tpcf;
    private final int[] sortFields;
    private final IBinaryComparatorFactory[] comparatorFactories;

    public LocalityAwareMToNPartitioningMergingConnectorDescriptor(JobSpecification spec,
            ITuplePartitionComputerFactory tpcf, ILocalityMap localityMap, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories) {
        super(spec);
        this.localityMap = localityMap;
        this.tpcf = tpcf;
        this.sortFields = sortFields;
        this.comparatorFactories = comparatorFactories;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor#createPartitioner
     * (edu.uci.ics.hyracks.api.context.IHyracksTaskContext,
     * edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor,
     * edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory, int, int, int)
     */
    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        return new LocalityAwarePartitionDataWriter(ctx, edwFactory, recordDesc, tpcf.createPartitioner(),
                nConsumerPartitions, localityMap, index);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor#
     * createPartitionCollector
     * (edu.uci.ics.hyracks.api.context.IHyracksTaskContext,
     * edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, int, int, int)
     */
    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            int receiverIndex, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        BitSet expectedPartitions = new BitSet(nProducerPartitions);
        for (int i = 0; i < nProducerPartitions; i++) {
            if (localityMap.isConnected(i, receiverIndex, nConsumerPartitions))
                expectedPartitions.set(i);
        }
        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        IPartitionBatchManager pbm = new NonDeterministicPartitionBatchManager(nProducerPartitions);
        IFrameReader sortMergeFrameReader = new SortMergeFrameReader(ctx, nProducerPartitions, nProducerPartitions,
                sortFields, comparators, recordDesc, pbm);

        return new PartitionCollector(ctx, getConnectorId(), receiverIndex, expectedPartitions, sortMergeFrameReader,
                pbm);
    }

}
