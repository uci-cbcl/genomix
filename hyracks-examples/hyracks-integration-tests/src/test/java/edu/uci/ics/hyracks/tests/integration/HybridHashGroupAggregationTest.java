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
import java.io.IOException;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.accessors.UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.AvgFieldGroupAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.AvgFieldMergeAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.FloatSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MinMaxStringFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartitionalloc.HybridHashDynamicPartitionAllocGroupOperatorDescriptor;

public class HybridHashGroupAggregationTest extends AbstractIntegrationTest {
    final IFileSplitProvider splitProvider = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC2_ID,
            new FileReference(new File("data/tpch0.001/lineitem.tbl"))) });
    //        new FileReference(new File("/Volumes/Home/Datasets/tpch/tpch0.1/lineitem.tbl"))) });

    final int inputSize = 3;

    final int[] testAlgorithms = new int[] { 4 };

    final RecordDescriptor desc = new RecordDescriptor(new ISerializerDeserializer[] {
            UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
            FloatSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

    final ITupleParserFactory tupleParserFactory = new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
            UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
            IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
            FloatParserFactory.INSTANCE, FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, }, '|');

    private AbstractSingleActivityOperatorDescriptor getPrinter(JobSpecification spec, String prefix)
            throws IOException {

        AbstractSingleActivityOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec,
                new ConstantFileSplitProvider(new FileSplit[] {
                        new FileSplit(NC1_ID, createTempFile().getAbsolutePath()),
                        new FileSplit(NC2_ID, createTempFile().getAbsolutePath()) }), "\t");

        return printer;
    }

    @Test
    public void singleKeySumHybridHashSortTest() throws Exception {
        for (int i : testAlgorithms) {
            JobSpecification spec = new JobSpecification();

            FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider,
                    tupleParserFactory, desc);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

            RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                    UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE });
            int estimatedRecSize = 10 + 4 + 4 + 4 + 4 * 4 + 4;

            int[] keyFields = new int[] { 0 };
            int framesLimit = 6;
            int tableSize = 4096;

            HybridHashDynamicPartitionAllocGroupOperatorDescriptor grouper = new HybridHashDynamicPartitionAllocGroupOperatorDescriptor(
                    spec,
                    keyFields,
                    framesLimit,
                    inputSize,
                    tableSize,
                    estimatedRecSize,
                    new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                    new FieldHashPartitionComputerFamily(
                            keyFields,
                            new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE }),
                    new FieldHashPartitionComputerFamily(
                            keyFields,
                            new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE }),
                    new UTF8StringNormalizedKeyComputerFactory(), new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                    new IntSumFieldAggregatorFactory(3, false),
                                    new FloatSumFieldAggregatorFactory(5, false) }), new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                    new IntSumFieldAggregatorFactory(2, false),
                                    new FloatSumFieldAggregatorFactory(3, false) }), outputRec, i);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

            IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keyFields,
                            new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                    .of(UTF8StringPointable.FACTORY) }));
            spec.connect(conn1, csvScanner, 0, grouper, 0);

            AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "singleKeySumHybridHashSortTest_" + i);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

            IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
            spec.connect(conn2, grouper, 0, printer, 0);

            spec.addRoot(printer);
            runTest(spec);
        }
    }

    @Test
    public void singleKeyAvgHybridHashSortTest() throws Exception {
        for (int i : testAlgorithms) {
            JobSpecification spec = new JobSpecification();

            FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider,
                    tupleParserFactory, desc);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

            RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                    UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE });
            int estimatedRecSize = 10 + 4 + 4 + 4 + 4 * 4 + 4;

            int[] keyFields = new int[] { 0 };
            int framesLimit = 6;
            int tableSize = 4096;

            HybridHashDynamicPartitionAllocGroupOperatorDescriptor grouper = new HybridHashDynamicPartitionAllocGroupOperatorDescriptor(
                    spec,
                    keyFields,
                    framesLimit,
                    inputSize,
                    tableSize,
                    estimatedRecSize,
                    new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                    new FieldHashPartitionComputerFamily(
                            keyFields,
                            new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE }),
                    new FieldHashPartitionComputerFamily(
                            keyFields,
                            new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE }),
                    new UTF8StringNormalizedKeyComputerFactory(), new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                    new CountFieldAggregatorFactory(false),
                                    new AvgFieldGroupAggregatorFactory(1, false) }), new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                    new IntSumFieldAggregatorFactory(2, false),
                                    new AvgFieldMergeAggregatorFactory(3, false) }), outputRec, i);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

            IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keyFields,
                            new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                    .of(UTF8StringPointable.FACTORY) }));
            spec.connect(conn1, csvScanner, 0, grouper, 0);

            AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "singleKeyAvgHybridHashSortTest_" + i);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

            IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
            spec.connect(conn2, grouper, 0, printer, 0);

            spec.addRoot(printer);
            runTest(spec);
        }
    }

    @Test
    public void singleKeyMinMaxStringHybridHashSortTest() throws Exception {
        for (int i : testAlgorithms) {
            JobSpecification spec = new JobSpecification();

            FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider,
                    tupleParserFactory, desc);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

            RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                    UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                    UTF8StringSerializerDeserializer.INSTANCE });
            int estimatedRecSize = 10 + 4 + 44 + 4 * 3 + 4;

            int[] keyFields = new int[] { 0 };
            int framesLimit = 6;
            int tableSize = 4096;

            HybridHashDynamicPartitionAllocGroupOperatorDescriptor grouper = new HybridHashDynamicPartitionAllocGroupOperatorDescriptor(
                    spec,
                    keyFields,
                    framesLimit,
                    inputSize,
                    tableSize,
                    estimatedRecSize,
                    new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                    new FieldHashPartitionComputerFamily(
                            keyFields,
                            new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE }),
                    new FieldHashPartitionComputerFamily(
                            keyFields,
                            new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE }),
                    new UTF8StringNormalizedKeyComputerFactory(), new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                    new MinMaxStringFieldAggregatorFactory(15, true, true) }),
                    new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                            new IntSumFieldAggregatorFactory(1, false),
                            new MinMaxStringFieldAggregatorFactory(2, true, true) }), outputRec, i);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

            IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keyFields,
                            new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                    .of(UTF8StringPointable.FACTORY) }));
            spec.connect(conn1, csvScanner, 0, grouper, 0);

            AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
                    "singleKeyMinMaxStringHybridHashSortTest_" + i);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

            IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
            spec.connect(conn2, grouper, 0, printer, 0);

            spec.addRoot(printer);
            runTest(spec);
        }
    }

    @Test
    public void multiKeySumHybridHashSortTest() throws Exception {
        for (int i : testAlgorithms) {
            JobSpecification spec = new JobSpecification();

            FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider,
                    tupleParserFactory, desc);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

            RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                    UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
            int estimatedRecSize = 10 + 10 + 4 + 4 + 4 * 4 + 4;

            int[] keyFields = new int[] { 8, 0 };
            int[] storedKeyFields = new int[] { 0, 1 };
            int framesLimit = 6;
            int tableSize = 4096;

            HybridHashDynamicPartitionAllocGroupOperatorDescriptor grouper = new HybridHashDynamicPartitionAllocGroupOperatorDescriptor(
                    spec, keyFields, framesLimit, inputSize, tableSize, estimatedRecSize,
                    new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                            PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                    new FieldHashPartitionComputerFamily(keyFields, new IBinaryHashFunctionFamily[] {
                            UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE,
                            UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE }),
                    new FieldHashPartitionComputerFamily(storedKeyFields, new IBinaryHashFunctionFamily[] {
                            UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE,
                            UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE }),
                    new UTF8StringNormalizedKeyComputerFactory(), new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                    new IntSumFieldAggregatorFactory(3, false) }), new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(2, false),
                                    new IntSumFieldAggregatorFactory(3, false) }), outputRec, i);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

            IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keyFields, new IBinaryHashFunctionFactory[] {
                            PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
                            PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
            spec.connect(conn1, csvScanner, 0, grouper, 0);

            AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "multiKeySumHybridHashSortTest_" + i);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

            IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
            spec.connect(conn2, grouper, 0, printer, 0);

            spec.addRoot(printer);
            runTest(spec);
        }
    }

    @Test
    public void multiKeyAvgHybridHashSortTest() throws Exception {
        for (int i : testAlgorithms) {
            JobSpecification spec = new JobSpecification();

            FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider,
                    tupleParserFactory, desc);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

            RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                    UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                    FloatSerializerDeserializer.INSTANCE });
            int estimatedRecSize = 10 + 10 + 4 + 4 + 4 + 4 * 5 + 4;

            int[] keyFields = new int[] { 8, 0 };
            int[] storedKeyFields = new int[] { 0, 1 };
            int framesLimit = 6;
            int tableSize = 4096;

            HybridHashDynamicPartitionAllocGroupOperatorDescriptor grouper = new HybridHashDynamicPartitionAllocGroupOperatorDescriptor(
                    spec, keyFields, framesLimit, inputSize, tableSize, estimatedRecSize,
                    new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                            PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                    new FieldHashPartitionComputerFamily(keyFields, new IBinaryHashFunctionFamily[] {
                            UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE,
                            UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE }),
                    new FieldHashPartitionComputerFamily(storedKeyFields, new IBinaryHashFunctionFamily[] {
                            UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE,
                            UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE }),
                    new UTF8StringNormalizedKeyComputerFactory(), new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                    new CountFieldAggregatorFactory(false),
                                    new AvgFieldGroupAggregatorFactory(1, false) }), new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(2, false),
                                    new IntSumFieldAggregatorFactory(3, false),
                                    new AvgFieldMergeAggregatorFactory(4, false) }), outputRec, i);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

            IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keyFields, new IBinaryHashFunctionFactory[] {
                            PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
                            PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
            spec.connect(conn1, csvScanner, 0, grouper, 0);

            AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "multiKeyAvgHybridHashSortTest_" + i);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

            IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
            spec.connect(conn2, grouper, 0, printer, 0);

            spec.addRoot(printer);
            runTest(spec);
        }
    }

    @Test
    public void multiKeyMinMaxStringHybridHashSortTest() throws Exception {
        for (int i : testAlgorithms) {
            JobSpecification spec = new JobSpecification();

            FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider,
                    tupleParserFactory, desc);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

            RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                    UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });
            int estimatedRecSize = 10 + 10 + 4 + 44 + 4 * 4 + 4;

            int[] keyFields = new int[] { 8, 0 };
            int[] storedKeyFields = new int[] { 0, 1 };
            int framesLimit = 6;
            int tableSize = 4096;

            HybridHashDynamicPartitionAllocGroupOperatorDescriptor grouper = new HybridHashDynamicPartitionAllocGroupOperatorDescriptor(
                    spec, keyFields, framesLimit, inputSize, tableSize, estimatedRecSize,
                    new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                            PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                    new FieldHashPartitionComputerFamily(keyFields, new IBinaryHashFunctionFamily[] {
                            UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE,
                            UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE }),
                    new FieldHashPartitionComputerFamily(storedKeyFields, new IBinaryHashFunctionFamily[] {
                            UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE,
                            UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper.INSTANCE }),
                    new UTF8StringNormalizedKeyComputerFactory(), new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                    new MinMaxStringFieldAggregatorFactory(15, true, true) }),
                    new MultiFieldsAggregatorFactory(new int[] { 0, 1 }, new IFieldAggregateDescriptorFactory[] {
                            new IntSumFieldAggregatorFactory(2, false),
                            new MinMaxStringFieldAggregatorFactory(3, true, true) }), outputRec, i);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

            IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keyFields, new IBinaryHashFunctionFactory[] {
                            PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
                            PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
            spec.connect(conn1, csvScanner, 0, grouper, 0);

            AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
                    "multiKeyMinMaxStringHybridHashSortTest_" + i);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

            IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
            spec.connect(conn2, grouper, 0, printer, 0);

            spec.addRoot(printer);
            runTest(spec);
        }
    }
}
