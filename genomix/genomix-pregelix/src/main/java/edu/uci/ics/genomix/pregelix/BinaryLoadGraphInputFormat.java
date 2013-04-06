package edu.uci.ics.genomix.pregelix;

import java.io.IOException;

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
import edu.uci.ics.genomix.pregelix.example.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexInputFormat;
import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexInputFormat.BinaryVertexReader;

public class BinaryLoadGraphInputFormat extends
	BinaryVertexInputFormat<BytesWritable, ByteWritable, NullWritable, MessageWritable>{
	static int number = 0;
	/**
	 * Format INPUT
	 */
    @Override
    public VertexReader<BytesWritable, ByteWritable, NullWritable, MessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
    	try{
    	System.out.println("split: "  + number++ + " length:"+ split.getLength());
    	}catch (Exception ex){
    		
    	}
        return new BinaryLoadGraphReader(binaryInputFormat.createRecordReader(split, context));
    }	
}

@SuppressWarnings("rawtypes")
class BinaryLoadGraphReader extends
        BinaryVertexReader<BytesWritable, ByteWritable, NullWritable, MessageWritable> {
    private Vertex vertex;
    private BytesWritable vertexId = new BytesWritable();
    private ByteWritable vertexValue = new ByteWritable();

    public BinaryLoadGraphReader(RecordReader<BytesWritable,KmerCountValue> recordReader) {
        super(recordReader);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return getRecordReader().nextKeyValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex<BytesWritable, ByteWritable, NullWritable, MessageWritable> getCurrentVertex() throws IOException,
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
            vertexValue.set(kmerCountValue.getAdjBitMap()); 
            vertex.setVertexValue(vertexValue);
        }
        
        return vertex;
    }
}
