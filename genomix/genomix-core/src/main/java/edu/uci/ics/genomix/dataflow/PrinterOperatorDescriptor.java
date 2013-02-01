package edu.uci.ics.genomix.dataflow;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.util.StringSerializationUtils;

public class PrinterOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private String filename;
    private boolean writeFile;
    private BufferedWriter writer;
    private FileOutputStream stream;

    public PrinterOperatorDescriptor(IOperatorDescriptorRegistry spec) {
        super(spec, 1, 0);
        writeFile = false;
    }
    
    public PrinterOperatorDescriptor(IOperatorDescriptorRegistry spec, String filename) {
        super(spec, 1, 0);
        this.filename = filename;
        writeFile = true;
    }

    private class PrinterOperator implements IOpenableDataWriterOperator {
        
    	private int partition;
    	public PrinterOperator(int partition){
    		this.partition = partition;
    	}
    	
    	@Override
        public void open() throws HyracksDataException {
        	if( true == writeFile){
        		try {
					filename = filename + String.valueOf(partition)+".txt";
					//System.err.println(filename);
        			stream = new FileOutputStream(filename);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                writer = new BufferedWriter(new OutputStreamWriter(stream));
        	}
        }

        @Override
        public void close() throws HyracksDataException {
        	//System.err.println("kick");
        	if( true == writeFile){
        		try {
        			writer.close();
					stream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	}
        }

        @Override
        public void fail() throws HyracksDataException {
        }

        @Override
        public void writeData(Object[] data) throws HyracksDataException {
        	try{
	        	if(true == writeFile){
		            for (int i = 0; i < data.length; ++i) {
		            	writer.write(String.valueOf(data[i]));
		            	writer.write(", ");
		            	writer.write("\n");
		            }
	        	}
	        	else{
		            for (int i = 0; i < data.length; ++i) {
		                System.err.print(StringSerializationUtils.toString(data[i]));
		                System.err.print(", ");
		            }
		            System.err.println();
	        	}
        	} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }

        @Override
        public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new DeserializedOperatorNodePushable(ctx, new PrinterOperator(partition),
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
    }
}