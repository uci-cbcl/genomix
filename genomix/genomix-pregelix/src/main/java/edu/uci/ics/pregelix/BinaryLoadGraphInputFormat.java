package edu.uci.ics.pregelix;

import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.io.binary.BinaryVertexInputFormat;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.bitwise.BitwiseOperation;
import edu.uci.ics.pregelix.example.io.MessageWritable;
import edu.uci.ics.pregelix.type.KmerCountValue;

public class BinaryLoadGraphInputFormat extends
	BinaryVertexInputFormat<BytesWritable, ByteWritable, NullWritable, MessageWritable>{

	/**
	 * Format INPUT
	 */
    @Override
    public VertexReader<BytesWritable, ByteWritable, NullWritable, MessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new BinaryLoadGraphReader(binaryInputFormat.createRecordReader(split, context));
    }
    
    @SuppressWarnings("rawtypes")
    class BinaryLoadGraphReader extends
            BinaryVertexReader<BytesWritable, ByteWritable, NullWritable, MessageWritable> {
        private final static String separator = " ";
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
            
            
            if(getRecordReader() != null){
	            /**
	             * set the src vertex id
	             */
            	/*vertexId = getRecordReader().getCurrentKey();
            	byte[] vertexBytes = vertexId.getBytes();
            	int numOfByte = (2*GraphVertexOperation.k-1)/8 + 1;
        		if(vertexBytes.length == numOfByte)
        			vertex.setVertexId(vertexId);
        		else{
        			byte[] tmp = new byte[numOfByte];
        			for(int i = 0; i < numOfByte; i++)
        				tmp[i] = vertexBytes[i];
        			vertex.setVertexId(new BytesWritable(tmp));
        		}*/
	            
        		vertexId = getRecordReader().getCurrentKey();
        		vertex.setVertexId(vertexId);
	            /**
	             * set the vertex value
	             */
	            KmerCountValue kmerCountValue = getRecordReader().getCurrentValue();
	            vertexValue.set(kmerCountValue.getAdjBitMap()); 
	            vertex.setVertexValue(vertexValue);
	            
	        	String kmer = BitwiseOperation.convertBytesToBinaryStringKmer(vertexId.getBytes(),GraphVertexOperation.k);
			    System.out.println("key: " + kmer);
			    System.out.println("code: " + GraphVertexOperation.convertBinaryStringToGenecode(kmer));
			    System.out.println("value: " + BitwiseOperation.convertByteToBinaryString(kmerCountValue.getAdjBitMap()));
			    System.out.println();
            }
            
            return vertex;
        }
    }
	
}
