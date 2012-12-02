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
package edu.uci.ics.hyracks.tests.integration;

import java.io.File;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.shuffle.RandomRowShufflerInputOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.shuffle.RandomRowShufflerOutputOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;

public class RowShufflerTest extends AbstractIntegrationTest {

    private void createPartitionConstraint(JobSpecification spec, IOperatorDescriptor op, FileSplit[] splits) {
        String[] partitions = new String[splits.length];
        for (int i = 0; i < splits.length; i++) {
            partitions[i] = splits[i].getNodeName();
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, op, partitions);
    }

    @Test
    public void rowShufflerTest01() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] inputFile = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/IPv6Float.dat"))) };

        FileScanOperatorDescriptor fileScanner = new FileScanOperatorDescriptor(spec, new ConstantFileSplitProvider(
                inputFile), new DelimitedDataTupleParserFactory(
                new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, '&'), new RecordDescriptor(
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE }));
        createPartitionConstraint(spec, fileScanner, inputFile);

        RandomRowShufflerInputOperatorDescriptor inShuffler = new RandomRowShufflerInputOperatorDescriptor(spec,
                20121127);

        IConnectorDescriptor loadShufflerConnector = new OneToOneConnectorDescriptor(spec);

        spec.connect(loadShufflerConnector, fileScanner, 0, inShuffler, 0);

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, 4, new int[] { 0 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(LongPointable.FACTORY) },
                new RecordDescriptor(new ISerializerDeserializer[] { Integer64SerializerDeserializer.INSTANCE,
                        UTF8StringSerializerDeserializer.INSTANCE }));

        IConnectorDescriptor shufflerSorterConnector = new OneToOneConnectorDescriptor(spec);

        spec.connect(shufflerSorterConnector, inShuffler, 0, sorter, 0);

        RandomRowShufflerOutputOperatorDescriptor outShuffler = new RandomRowShufflerOutputOperatorDescriptor(spec);

        IConnectorDescriptor sorterOutShuffler = new OneToOneConnectorDescriptor(spec);

        spec.connect(sorterOutShuffler, sorter, 0, outShuffler, 0);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });

        AbstractOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, "");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor shufflerWriterConnector = new OneToOneConnectorDescriptor(spec);
        spec.connect(shufflerWriterConnector, outShuffler, 0, printer, 0);

        runTest(spec);
    }

}
