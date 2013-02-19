package edu.uci.ics.genomix.job;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.genomix.data.normalizers.VLongNormalizedKeyComputerFactory;
import edu.uci.ics.genomix.data.partition.KmerHashPartitioncomputerFactory;
import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
import edu.uci.ics.genomix.data.std.accessors.VLongBinaryHashFunctionFamily;
import edu.uci.ics.genomix.data.std.primitive.VLongPointable;
import edu.uci.ics.genomix.dataflow.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.genomix.dataflow.KMerSequenceWriterFactory;
import edu.uci.ics.genomix.dataflow.KMerTextWriterFactory;
import edu.uci.ics.genomix.dataflow.KMerWriterFactory;
import edu.uci.ics.genomix.dataflow.ReadsKeyValueParserFactory;
import edu.uci.ics.genomix.dataflow.aggregators.DistributedMergeLmerAggregateFactory;
import edu.uci.ics.genomix.dataflow.aggregators.MergeKmerAggregateFactory;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashSpillableTableFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.external.ExternalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.HybridHashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.preclustered.PreclusteredGroupOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSWriteOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

public class JobGenBrujinGraph extends JobGen {
	public enum GroupbyType {
		EXTERNAL, PRECLUSTER, HYBRIDHASH,
	}
	public enum OutputFormat{
		TEXT,BINARY,
	}

	private static final Log LOG = LogFactory.getLog(JobGenBrujinGraph.class);
	private final Map<String, NodeControllerInfo> ncMap;
	private Scheduler scheduler;
	private String[] ncNodeNames;

	private int kmers;
	private int frameLimits;
	private int tableSize;
	private GroupbyType groupbyType;
	private OutputFormat outputFormat;

	private AbstractOperatorDescriptor singleGrouper;
	private IConnectorDescriptor connPartition;
	private AbstractOperatorDescriptor crossGrouper;
	private RecordDescriptor outputRec;

	public JobGenBrujinGraph(GenomixJob job, Scheduler scheduler,
			final Map<String, NodeControllerInfo> ncMap,
			int numPartitionPerMachine) {
		super(job);
		this.ncMap = ncMap;
		this.scheduler = scheduler;
		String[] nodes = new String[ncMap.size()];
		ncMap.keySet().toArray(nodes);
		ncNodeNames = new String[nodes.length * numPartitionPerMachine];
		for (int i = 0; i < numPartitionPerMachine; i++) {
			System.arraycopy(nodes, 0, ncNodeNames, i * nodes.length,
					nodes.length);
		}
		LOG.info("nc nodes:" + ncNodeNames.length + ncNodeNames.toString());
	}

	private ExternalGroupOperatorDescriptor newExternalGroupby(
			JobSpecification jobSpec, int[] keyFields,
			IAggregatorDescriptorFactory aggeragater) {
		return new ExternalGroupOperatorDescriptor(
				jobSpec,
				keyFields,
				frameLimits,
				new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
						.of(VLongPointable.FACTORY) },
				new VLongNormalizedKeyComputerFactory(),
				aggeragater,
				new DistributedMergeLmerAggregateFactory(),
				outputRec,
				new HashSpillableTableFactory(
						new FieldHashPartitionComputerFactory(
								keyFields,
								new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
										.of(VLongPointable.FACTORY) }),
						tableSize), true);
	}

	private HybridHashGroupOperatorDescriptor newHybridGroupby(
			JobSpecification jobSpec, int[] keyFields,
			long inputSizeInRawRecords, long inputSizeInUniqueKeys,
			int recordSizeInBytes, int hashfuncStartLevel)
			throws HyracksDataException {
		return new HybridHashGroupOperatorDescriptor(
				jobSpec,
				keyFields,
				frameLimits,
				inputSizeInRawRecords,
				inputSizeInUniqueKeys,
				recordSizeInBytes,
				tableSize,
				new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
						.of(VLongPointable.FACTORY) },
				new IBinaryHashFunctionFamily[] { new VLongBinaryHashFunctionFamily() },
				hashfuncStartLevel,
				new VLongNormalizedKeyComputerFactory(),
				new MergeKmerAggregateFactory(),
				new DistributedMergeLmerAggregateFactory(), outputRec, true);
	}

	private void generateDescriptorbyType(JobSpecification jobSpec)
			throws HyracksDataException {
		int[] keyFields = new int[] { 0 }; // the id of grouped key

		switch (groupbyType) {
		case EXTERNAL:
			singleGrouper = newExternalGroupby(jobSpec, keyFields,
					new MergeKmerAggregateFactory());
			connPartition = new MToNPartitioningConnectorDescriptor(jobSpec,
					new KmerHashPartitioncomputerFactory());
			crossGrouper = newExternalGroupby(jobSpec, keyFields,
					new DistributedMergeLmerAggregateFactory());
			break;
		case PRECLUSTER:
			singleGrouper = newExternalGroupby(jobSpec, keyFields,
					new MergeKmerAggregateFactory());
			connPartition = new MToNPartitioningMergingConnectorDescriptor(
					jobSpec,
					new KmerHashPartitioncomputerFactory(),
					keyFields,
					new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
							.of(VLongPointable.FACTORY) });
			crossGrouper = new PreclusteredGroupOperatorDescriptor(
					jobSpec,
					keyFields,
					new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
							.of(VLongPointable.FACTORY) },
					new DistributedMergeLmerAggregateFactory(), outputRec);
			break;
		case HYBRIDHASH:
		default:
			long inputSizeInRawRecords = conf.getLong(
					GenomixJob.GROUPBY_HYBRID_INPUTSIZE, 154000000);
			long inputSizeInUniqueKeys = conf.getLong(
					GenomixJob.GROUPBY_HYBRID_INPUTKEYS, 38500000);
			int recordSizeInBytes = conf.getInt(
					GenomixJob.GROUPBY_HYBRID_RECORDSIZE_SINGLE, 9);
			int hashfuncStartLevel = conf.getInt(
					GenomixJob.GROUPBY_HYBRID_HASHLEVEL, 1);

			singleGrouper = newHybridGroupby(jobSpec, keyFields,
					inputSizeInRawRecords, inputSizeInUniqueKeys,
					recordSizeInBytes, hashfuncStartLevel);
			connPartition = new MToNPartitioningConnectorDescriptor(jobSpec,
					new KmerHashPartitioncomputerFactory());
			/** here read the different recordSize why ? */
			recordSizeInBytes = conf.getInt(
					GenomixJob.GROUPBY_HYBRID_RECORDSIZE_CROSS, 13);
			crossGrouper = newHybridGroupby(jobSpec, keyFields,
					inputSizeInRawRecords, inputSizeInUniqueKeys,
					recordSizeInBytes, hashfuncStartLevel);
			break;
		}
	}

	public HDFSReadOperatorDescriptor createHDFSReader(JobSpecification jobSpec)
			throws HyracksDataException {
		try {
			InputSplit[] splits = ((JobConf) conf).getInputFormat().getSplits(
					(JobConf) conf, ncNodeNames.length);

			String[] readSchedule = scheduler.getLocationConstraints(splits);
			return new HDFSReadOperatorDescriptor(jobSpec, outputRec,
					(JobConf) conf, splits, readSchedule,
					new ReadsKeyValueParserFactory(kmers));
		} catch (Exception e) {
			throw new HyracksDataException(e);
		}
	}

	@Override
	public JobSpecification generateJob() throws HyracksException {

		JobSpecification jobSpec = new JobSpecification();
		outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
				null, ByteSerializerDeserializer.INSTANCE,
				ByteSerializerDeserializer.INSTANCE });
		// File input
		HDFSReadOperatorDescriptor readOperator = createHDFSReader(jobSpec);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec,
				readOperator, ncNodeNames);

		generateDescriptorbyType(jobSpec);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec,
				singleGrouper, ncNodeNames);

		IConnectorDescriptor readfileConn = new OneToOneConnectorDescriptor(
				jobSpec);
		jobSpec.connect(readfileConn, readOperator, 0, singleGrouper, 0);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec,
				crossGrouper, ncNodeNames);
		jobSpec.connect(connPartition, singleGrouper, 0, crossGrouper, 0);

		// Output
		ITupleWriterFactory writer = null;
		switch (outputFormat){
		case TEXT:
			writer = new KMerTextWriterFactory();
			break;
		case BINARY: default:
			writer = new KMerSequenceWriterFactory(conf);
			break;
		}
		HDFSWriteOperatorDescriptor writeOperator = new HDFSWriteOperatorDescriptor(
				jobSpec, (JobConf) conf, writer);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec,
				writeOperator, ncNodeNames);

		IConnectorDescriptor printConn = new OneToOneConnectorDescriptor(
				jobSpec);
		jobSpec.connect(printConn, crossGrouper, 0, writeOperator, 0);
		jobSpec.addRoot(writeOperator);

		if (groupbyType == GroupbyType.PRECLUSTER) {
			jobSpec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
		}
		return jobSpec;
	}

	@Override
	protected void initJobConfiguration() {
		kmers = conf.getInt(GenomixJob.KMER_LENGTH, 25);
		frameLimits = conf.getInt(GenomixJob.FRAME_LIMIT, 4096);
		tableSize = conf.getInt(GenomixJob.TABLE_SIZE, 10485767);

		String type = conf.get(GenomixJob.GROUPBY_TYPE, "hybrid");
		if (type.equalsIgnoreCase("external")) {
			groupbyType = GroupbyType.EXTERNAL;
		} else if (type.equalsIgnoreCase("precluster")) {
			groupbyType = GroupbyType.PRECLUSTER;
		} else {
			groupbyType = GroupbyType.HYBRIDHASH;
		}
		
		String output = conf.get(GenomixJob.OUTPUT_FORMAT, "binary");
		if (output.equalsIgnoreCase("binary")){
			outputFormat = OutputFormat.BINARY;
		} else if ( output.equalsIgnoreCase("text")){
			outputFormat = OutputFormat.TEXT;
		} else {
			outputFormat = OutputFormat.TEXT;
		}
	}

}
