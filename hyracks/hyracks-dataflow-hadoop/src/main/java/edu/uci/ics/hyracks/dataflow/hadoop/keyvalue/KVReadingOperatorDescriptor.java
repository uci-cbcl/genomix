package edu.uci.ics.hyracks.dataflow.hadoop.keyvalue;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class KVReadingOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public KVReadingOperatorDescriptor(IOperatorDescriptorRegistry spec,
			int inputArity, int outputArity) {
		super(spec, inputArity, outputArity);
		// TODO Auto-generated constructor stub
	}

	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
			IRecordDescriptorProvider recordDescProvider, int partition,
			int nPartitions) throws HyracksDataException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * 
				
		//Get the schema from each operator and populate the fields accordingly
		ISerializerDeserializer[] fields = new ISerializerDeserializer[2];
		Writable keyClass = args[1];
        Writable valueClass = args[2];
		fields[0] = createSerializerDeserializer(keyClass);
		fields[1] = createSerializerDeserializer(valueClass);
		RecordDescriptor recDes = new RecordDescriptor(fields);
	
	 * */
}
