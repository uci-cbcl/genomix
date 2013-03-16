package edu.uci.ics.pregelix;

import java.io.IOException;

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
import edu.uci.ics.pregelix.example.io.LogAlgorithmMessageWritable;
import edu.uci.ics.pregelix.example.io.ValueStateWritable;
import edu.uci.ics.pregelix.type.KmerCountValue;
import edu.uci.ics.pregelix.type.State;

public class LogAlgorithmForMergeGraphInputFormat extends
	BinaryVertexInputFormat<BytesWritable, ValueStateWritable, NullWritable, LogAlgorithmMessageWritable>{

	/**
	 * Format INPUT
	 */
    @Override
    public VertexReader<BytesWritable, ValueStateWritable, NullWritable, LogAlgorithmMessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new BinaryLoadGraphReader(binaryInputFormat.createRecordReader(split, context));
    }
    
    @SuppressWarnings("rawtypes")
    class BinaryLoadGraphReader extends
            BinaryVertexReader<BytesWritable, ValueStateWritable, NullWritable, LogAlgorithmMessageWritable> {
        private final static String separator = " ";
        private Vertex vertex;
        private BytesWritable vertexId = new BytesWritable();
        private ValueStateWritable vertexValue = new ValueStateWritable();

        public BinaryLoadGraphReader(RecordReader<BytesWritable,KmerCountValue> recordReader) {
            super(recordReader);
        }

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Vertex<BytesWritable, ValueStateWritable, NullWritable, LogAlgorithmMessageWritable> getCurrentVertex() throws IOException,
                InterruptedException {
            if (vertex == null)
                vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());

            vertex.getMsgList().clear();
            vertex.getEdges().clear();
            
            
            if(getRecordReader() != null){
	            /**
	             * set the src vertex id
	             */
	            
        		vertexId = getRecordReader().getCurrentKey();
        		vertex.setVertexId(vertexId);
	            /**
	             * set the vertex value
	             */
	            KmerCountValue kmerCountValue = getRecordReader().getCurrentValue();
	            vertexValue.setValue(kmerCountValue.getAdjBitMap()); 
	            vertexValue.setState(State.NON_VERTEX);
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
