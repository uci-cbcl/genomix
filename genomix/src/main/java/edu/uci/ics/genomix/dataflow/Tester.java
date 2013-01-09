package edu.uci.ics.genomix.dataflow;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.genomix.data.normalizers.Integer64NormalizedKeyComputerFactory;
import edu.uci.ics.genomix.data.partition.KmerHashPartitioncomputerFactory;
import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
import edu.uci.ics.genomix.dataflow.aggregators.DistributedMergeLmerAggregateFactory;
import edu.uci.ics.genomix.dataflow.aggregators.MergeKmerAggregateFactory;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashSpillableTableFactory;
import edu.uci.ics.hyracks.dataflow.std.group.external.ExternalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;

public class Tester {

    private static final Logger LOGGER = Logger.getLogger(Tester.class.getName());
    public static final String NC1_ID = "nc1";
    public static final String NC2_ID = "nc2";

    private static ClusterControllerService cc;
    private static NodeControllerService nc1;
    private static NodeControllerService nc2;
    private static IHyracksClientConnection hcc;

    //private static final boolean DEBUG = true;

    public static void main(String[] args) throws Exception {

        LOGGER.setLevel(Level.OFF);

        init();

        // Options options = new Options();

        IHyracksClientConnection hcc = new HyracksConnection("127.0.0.1", 39000);

        /*
         * JobSpecification job =
         * createJob(parseFileSplits(options.inFileCustomerSplits),
         * parseFileSplits(options.inFileOrderSplits),
         * parseFileSplits(options.outFileSplits), options.numJoinPartitions,
         * options.algo, options.graceInputSize, options.graceRecordsPerFrame,
         * options.graceFactor, options.memSize, options.tableSize,
         * options.hasGroupBy);
         */

        int k, page_num;
        String file_name = args[0];
        k = Integer.parseInt(args[1]);
        page_num = Integer.parseInt(args[2]);

        JobSpecification job = createJob(file_name, k, page_num);

        long start = System.currentTimeMillis();
        JobId jobId = hcc.startJob("test", job);
        hcc.waitForCompletion(jobId);
        long end = System.currentTimeMillis();
        System.err.println(start + " " + end + " " + (end - start));
    }

    //    private static FileSplit[] parseFileSplits(String fileSplits) {
    //        String[] splits = fileSplits.split(",");
    //        FileSplit[] fSplits = new FileSplit[splits.length];
    //        for (int i = 0; i < splits.length; ++i) {
    //            String s = splits[i].trim();
    //            int idx = s.indexOf(':');
    //            if (idx < 0) {
    //                throw new IllegalArgumentException("File split " + s + " not well formed");
    //            }
    //            fSplits[i] = new FileSplit(s.substring(0, idx), new FileReference(new File(s.substring(idx + 1))));
    //        }
    //        return fSplits;
    //    }

    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetPort = 39001;
        ccConfig.profileDumpPeriod = 10000;
        File outDir = new File("target/ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(Tester.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.ccRoot = ccRoot.getAbsolutePath();
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = "localhost";
        ncConfig1.ccPort = 39001;
        ncConfig1.clusterNetIPAddress = "127.0.0.1";
        ncConfig1.dataIPAddress = "127.0.0.1";
        ncConfig1.nodeId = NC1_ID;
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();

        NCConfig ncConfig2 = new NCConfig();
        ncConfig2.ccHost = "localhost";
        ncConfig2.ccPort = 39001;
        ncConfig2.clusterNetIPAddress = "127.0.0.1";
        ncConfig2.dataIPAddress = "127.0.0.1";
        ncConfig2.nodeId = NC2_ID;
        nc2 = new NodeControllerService(ncConfig2);
        nc2.start();

        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
        hcc.createApplication("test", null);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting CC in " + ccRoot.getAbsolutePath());
        }
    }

    private static JobSpecification createJob(String filename, int k, int page_num) throws HyracksDataException {
        JobSpecification spec = new JobSpecification();

        /*
         * IFileSplitProvider custSplitsProvider = new
         * ConstantFileSplitProvider(customerSplits);
         * //ConstantFileSplitProvider is the operator to provide the tuples
         * RecordDescriptor custDesc = new RecordDescriptor(new
         * ISerializerDeserializer[] {
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE });
         * 
         * IFileSplitProvider ordersSplitsProvider = new
         * ConstantFileSplitProvider(orderSplits); RecordDescriptor ordersDesc =
         * new RecordDescriptor(new ISerializerDeserializer[] {
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE });
         * 
         * RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new
         * ISerializerDeserializer[] {
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE,
         * UTF8StringSerializerDeserializer.INSTANCE });
         * 
         * FileScanOperatorDescriptor ordScanner = new
         * FileScanOperatorDescriptor(spec, ordersSplitsProvider, new
         * DelimitedDataTupleParserFactory(new IValueParserFactory[] {
         * UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
         * UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
         * UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
         * UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
         * UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
         * createPartitionConstraint(spec, ordScanner, orderSplits);
         * 
         * FileScanOperatorDescriptor custScanner = new
         * FileScanOperatorDescriptor(spec, custSplitsProvider, new
         * DelimitedDataTupleParserFactory(new IValueParserFactory[] {
         * UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
         * UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
         * UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
         * UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE },
         * '|'), custDesc); createPartitionConstraint(spec, custScanner,
         * customerSplits);
         * 
         * IOperatorDescriptor join;
         * 
         * if ("nestedloop".equalsIgnoreCase(algo)) { join = new
         * NestedLoopJoinOperatorDescriptor(spec, new JoinComparatorFactory(
         * PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0,
         * 1), custOrderJoinDesc, memSize);
         * 
         * } else if ("gracehash".equalsIgnoreCase(algo)) { join = new
         * GraceHashJoinOperatorDescriptor( spec, memSize, graceInputSize,
         * graceRecordsPerFrame, graceFactor, new int[] { 0 }, new int[] { 1 },
         * new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
         * .of(UTF8StringPointable.FACTORY) }, new IBinaryComparatorFactory[] {
         * PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
         * custOrderJoinDesc);
         * 
         * } else if ("hybridhash".equalsIgnoreCase(algo)) { join = new
         * HybridHashJoinOperatorDescriptor( spec, memSize, graceInputSize,
         * graceRecordsPerFrame, graceFactor, new int[] { 0 }, new int[] { 1 },
         * new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
         * .of(UTF8StringPointable.FACTORY) }, new IBinaryComparatorFactory[] {
         * PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
         * custOrderJoinDesc);
         * 
         * } else { join = new InMemoryHashJoinOperatorDescriptor( spec, new
         * int[] { 0 }, new int[] { 1 }, new IBinaryHashFunctionFactory[] {
         * PointableBinaryHashFunctionFactory .of(UTF8StringPointable.FACTORY)
         * }, new IBinaryComparatorFactory[] {
         * PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
         * custOrderJoinDesc, 6000000); }
         * 
         * PartitionConstraintHelper.addPartitionCountConstraint(spec, join,
         * numJoinPartitions);
         * 
         * IConnectorDescriptor ordJoinConn = new
         * MToNPartitioningConnectorDescriptor(spec, new
         * FieldHashPartitionComputerFactory(new int[] { 1 }, new
         * IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
         * .of(UTF8StringPointable.FACTORY) })); spec.connect(ordJoinConn,
         * ordScanner, 0, join, 1);
         * 
         * IConnectorDescriptor custJoinConn = new
         * MToNPartitioningConnectorDescriptor(spec, new
         * FieldHashPartitionComputerFactory(new int[] { 0 }, new
         * IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
         * .of(UTF8StringPointable.FACTORY) })); spec.connect(custJoinConn,
         * custScanner, 0, join, 0);
         * 
         * IOperatorDescriptor endingOp = join;
         * 
         * if (hasGroupBy) {
         * 
         * RecordDescriptor groupResultDesc = new RecordDescriptor(new
         * ISerializerDeserializer[] {
         * UTF8StringSerializerDeserializer.INSTANCE,
         * IntegerSerializerDeserializer.INSTANCE });
         * 
         * HashGroupOperatorDescriptor gby = new HashGroupOperatorDescriptor(
         * spec, new int[] { 6 }, new FieldHashPartitionComputerFactory(new
         * int[] { 6 }, new IBinaryHashFunctionFactory[] {
         * PointableBinaryHashFunctionFactory .of(UTF8StringPointable.FACTORY)
         * }), new IBinaryComparatorFactory[] {
         * PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
         * new MultiFieldsAggregatorFactory( new
         * IFieldAggregateDescriptorFactory[] { new
         * CountFieldAggregatorFactory(true) }), groupResultDesc, 16);
         * createPartitionConstraint(spec, gby, resultSplits);
         * 
         * IConnectorDescriptor joinGroupConn = new
         * MToNPartitioningConnectorDescriptor(spec, new
         * FieldHashPartitionComputerFactory(new int[] { 6 }, new
         * IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
         * .of(UTF8StringPointable.FACTORY) })); spec.connect(joinGroupConn,
         * join, 0, gby, 0);
         * 
         * endingOp = gby; }
         */

        // IOperatorDescriptor printer = DEBUG ? new
        // PrinterOperatorDescriptor(spec)
        // : new NullSinkOperatorDescriptor(spec);
        // PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
        // printer, NC1_ID);

        spec.setFrameSize(128);

        FileScanDescriptor scan = new FileScanDescriptor(spec, k);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, scan, NC1_ID, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                Integer64SerializerDeserializer.INSTANCE, ByteSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE });

        int[] keyFields = new int[] { 0 };
        int frameLimits = 10;
        int tableSize = 128;

        ExternalGroupOperatorDescriptor single_grouper = new ExternalGroupOperatorDescriptor(spec, keyFields,
                frameLimits,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(LongPointable.FACTORY) },
                new Integer64NormalizedKeyComputerFactory(), new MergeKmerAggregateFactory(),
                // new IntSumFieldAggregatorFactory(1, false) }),

                new MergeKmerAggregateFactory(),
                // new IntSumFieldAggregatorFactory(1, false) }),
                outputRec, new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(keyFields,
                                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                        .of(LongPointable.FACTORY) }), tableSize), true);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, single_grouper, NC1_ID, NC2_ID);

        IConnectorDescriptor readfileConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(readfileConn, scan, 0, single_grouper, 0);

        IConnectorDescriptor conn_partition = new MToNPartitioningConnectorDescriptor(spec,
                new KmerHashPartitioncomputerFactory());

        ExternalGroupOperatorDescriptor cross_grouper = new ExternalGroupOperatorDescriptor(spec, keyFields,
                frameLimits,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(LongPointable.FACTORY) },
                new Integer64NormalizedKeyComputerFactory(), new DistributedMergeLmerAggregateFactory(),
                // new IntSumFieldAggregatorFactory(1, false) }),

                new DistributedMergeLmerAggregateFactory(),
                // new IntSumFieldAggregatorFactory(1, false) }),
                outputRec, new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(keyFields,
                                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                        .of(LongPointable.FACTORY) }), tableSize), true);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, cross_grouper, NC1_ID, NC2_ID);
        spec.connect(conn_partition, single_grouper, 0, cross_grouper, 0);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID, NC2_ID);

        IConnectorDescriptor printConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(printConn, cross_grouper, 0, printer, 0);

        //spec.connect(printConn, cross_grouper, 0, printer, 0);

        /*
         * GenKmerDescriptor kmerGen = new GenKmerDescriptor(spec, page_num, k);
         * PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
         * kmerGen, NC1_ID);
         * 
         * IConnectorDescriptor readfileConn = new
         * OneToOneConnectorDescriptor(spec); spec.connect(readfileConn, scan,
         * 0, kmerGen, 0);
         * 
         * PrinterOperatorDescriptor printer = new
         * PrinterOperatorDescriptor(spec);
         * PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
         * printer, NC1_ID);
         * 
         * IConnectorDescriptor printConn = new
         * OneToOneConnectorDescriptor(spec); //spec.connect(printConn, scan, 0,
         * printer, 0);
         * 
         * //IConnectorDescriptor printConn = new
         * OneToOneConnectorDescriptor(spec); spec.connect(printConn, kmerGen,
         * 0, printer, 0);
         * 
         * //IFileSplitProvider outSplitProvider = new
         * ConstantFileSplitProvider(resultSplits);
         * //FrameFileWriterOperatorDescriptor writer = new
         * FrameFileWriterOperatorDescriptor(spec, outSplitProvider);
         * //createPartitionConstraint(spec, writer, resultSplits);
         * 
         * //IConnectorDescriptor endingPrinterConn = new
         * OneToOneConnectorDescriptor(spec); //spec.connect(endingPrinterConn,
         * endingOp, 0, writer, 0);
         */

        spec.addRoot(printer);

        // System.out.println(spec.toString());
        return spec;
    }

    //    private static void createPartitionConstraint(JobSpecification spec, IOperatorDescriptor op, FileSplit[] splits) {
    //        String[] parts = new String[splits.length];
    //        for (int i = 0; i < splits.length; ++i) {
    //            parts[i] = splits[i].getNodeName();
    //        }
    //        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, op, parts);
    //    }

    static class JoinComparatorFactory implements ITuplePairComparatorFactory {
        private static final long serialVersionUID = 1L;

        private final IBinaryComparatorFactory bFactory;
        private final int pos0;
        private final int pos1;

        public JoinComparatorFactory(IBinaryComparatorFactory bFactory, int pos0, int pos1) {
            this.bFactory = bFactory;
            this.pos0 = pos0;
            this.pos1 = pos1;
        }

        @Override
        public ITuplePairComparator createTuplePairComparator(IHyracksTaskContext ctx) {
            return new JoinComparator(bFactory.createBinaryComparator(), pos0, pos1);
        }
    }

    static class JoinComparator implements ITuplePairComparator {

        private final IBinaryComparator bComparator;
        private final int field0;
        private final int field1;

        public JoinComparator(IBinaryComparator bComparator, int field0, int field1) {
            this.bComparator = bComparator;
            this.field0 = field0;
            this.field1 = field1;
        }

        @Override
        public int compare(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1) {
            int tStart0 = accessor0.getTupleStartOffset(tIndex0);
            int fStartOffset0 = accessor0.getFieldSlotsLength() + tStart0;

            int tStart1 = accessor1.getTupleStartOffset(tIndex1);
            int fStartOffset1 = accessor1.getFieldSlotsLength() + tStart1;

            int fStart0 = accessor0.getFieldStartOffset(tIndex0, field0);
            int fEnd0 = accessor0.getFieldEndOffset(tIndex0, field0);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = accessor1.getFieldStartOffset(tIndex1, field1);
            int fEnd1 = accessor1.getFieldEndOffset(tIndex1, field1);
            int fLen1 = fEnd1 - fStart1;

            int c = bComparator.compare(accessor0.getBuffer().array(), fStart0 + fStartOffset0, fLen0, accessor1
                    .getBuffer().array(), fStart1 + fStartOffset1, fLen1);
            if (c != 0) {
                return c;
            }
            return 0;
        }
    }

}
