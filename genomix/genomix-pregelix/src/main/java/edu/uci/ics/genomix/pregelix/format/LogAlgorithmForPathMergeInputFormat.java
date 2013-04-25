package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexInputFormat;
import edu.uci.ics.genomix.pregelix.io.LogAlgorithmMessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerCountValue;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class LogAlgorithmForPathMergeInputFormat extends
	BinaryVertexInputFormat<KmerBytesWritable, ValueStateWritable, NullWritable, LogAlgorithmMessageWritable>{
	/**
	 * Format INPUT
	 */
    @SuppressWarnings("unchecked")
	@Override
    public VertexReader<KmerBytesWritable, ValueStateWritable, NullWritable, LogAlgorithmMessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new BinaryLoadGraphReader(binaryInputFormat.createRecordReader(split, context));
    }
    
    @SuppressWarnings("rawtypes")
    class BinaryLoadGraphReader extends
            BinaryVertexReader<KmerBytesWritable, ValueStateWritable, NullWritable, LogAlgorithmMessageWritable> {
        private Vertex vertex = null;
        private KmerBytesWritable vertexId = null;
        private ValueStateWritable vertexValue = new ValueStateWritable();

        public BinaryLoadGraphReader(RecordReader<KmerBytesWritable,KmerCountValue> recordReader) {
            super(recordReader);
        }

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Vertex<KmerBytesWritable, ValueStateWritable, NullWritable, LogAlgorithmMessageWritable> getCurrentVertex() throws IOException,
                InterruptedException {
            if (vertex == null)
                vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());

            vertex.getMsgList().clear();
            vertex.getEdges().clear();
            
            if(getRecordReader() != null){
	            /**
	             * set the src vertex id
	             */
            	if(vertexId == null)
            		vertexId = new KmerBytesWritable(getRecordReader().getCurrentKey().getKmerLength());
        		vertexId.set(getRecordReader().getCurrentKey());
        		vertex.setVertexId(vertexId);
	            /**
	             * set the vertex value
	             */
	            KmerCountValue kmerCountValue = getRecordReader().getCurrentValue();
	            vertexValue.setAdjMap(kmerCountValue.getAdjBitMap()); 
	            vertexValue.setState(State.NON_VERTEX);
	            vertex.setVertexValue(vertexValue);
            }
            
            return vertex;
        }
    }
	
}
