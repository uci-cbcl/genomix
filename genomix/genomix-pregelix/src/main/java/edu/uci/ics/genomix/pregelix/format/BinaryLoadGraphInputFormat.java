package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.type.KmerCountValue;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.pregelix.GraphVertexOperation;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.io.ValueWritable;
import edu.uci.ics.genomix.pregelix.log.DataLoadLogFormatter;
import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexInputFormat;
import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexInputFormat.BinaryVertexReader;

public class BinaryLoadGraphInputFormat extends
	BinaryVertexInputFormat<BytesWritable, ValueStateWritable, NullWritable, MessageWritable>{
	/**
	 * Format INPUT
	 */
    @Override
    public VertexReader<BytesWritable, ValueStateWritable, NullWritable, MessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new BinaryLoadGraphReader(binaryInputFormat.createRecordReader(split, context));
    }	
}

@SuppressWarnings("rawtypes")
class BinaryLoadGraphReader extends
        BinaryVertexReader<BytesWritable, ValueStateWritable, NullWritable, MessageWritable> {
	public static Logger logger = Logger.getLogger(BinaryLoadGraphReader.class.getName()); 
	DataLoadLogFormatter formatter = new DataLoadLogFormatter();
	FileHandler handler;
    private Vertex vertex;
    private BytesWritable vertexId = new BytesWritable();
    private ValueStateWritable vertexValue = new ValueStateWritable();

    public BinaryLoadGraphReader(RecordReader<BytesWritable,KmerCountValue> recordReader) {
        super(recordReader);
		try {
			handler = new FileHandler("log/" + BinaryLoadGraphReader.class.getName() + ".log");
		} catch (SecurityException e1) { e1.printStackTrace();} 
		catch (IOException e1) { e1.printStackTrace();}
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return getRecordReader().nextKeyValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex<BytesWritable, ValueStateWritable, NullWritable, MessageWritable> getCurrentVertex() throws IOException,
            InterruptedException {
        if (vertex == null)
            vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());

        vertex.getMsgList().clear();
        vertex.getEdges().clear();
        
        vertex.reset();
        if(getRecordReader() != null){
            /**
             * set the src vertex id
             */
    		vertexId.set(getRecordReader().getCurrentKey());
    		vertex.setVertexId(vertexId);
            /**
             * set the vertex value
             */
            KmerCountValue kmerCountValue = getRecordReader().getCurrentValue();
            vertexValue.setValue(kmerCountValue.getAdjBitMap()); 
            vertex.setVertexValue(vertexValue);
            
			//log
			formatter.set(vertexId, kmerCountValue, GraphVertexOperation.k);
			if(logger.getHandlers() != null)
				logger.removeHandler(handler);
			handler.setFormatter(formatter);
			logger.addHandler(handler);
			logger.info("");
        }
        
        return vertex;
    }
}
