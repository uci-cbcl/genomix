package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexOutputFormat;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;

public class NaiveAlgorithmForPathMergeOutputFormat extends 
	BinaryVertexOutputFormat<KmerBytesWritable, ValueStateWritable, NullWritable> {

        @Override
        public VertexWriter<KmerBytesWritable, ValueStateWritable, NullWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            @SuppressWarnings("unchecked")
			RecordWriter<KmerBytesWritable, ValueStateWritable> recordWriter = binaryOutputFormat.getRecordWriter(context);
            return new BinaryLoadGraphVertexWriter(recordWriter);
        }
        
        /**
         * Simple VertexWriter that supports {@link BinaryLoadGraphVertex}
         */
        public static class BinaryLoadGraphVertexWriter extends
                BinaryVertexWriter<KmerBytesWritable, ValueStateWritable, NullWritable> {
            public BinaryLoadGraphVertexWriter(RecordWriter<KmerBytesWritable, ValueStateWritable> lineRecordWriter) {
                super(lineRecordWriter);
            }

            @Override
            public void writeVertex(Vertex<KmerBytesWritable, ValueStateWritable, NullWritable, ?> vertex) throws IOException,
                    InterruptedException {
            	//if(vertex.getVertexValue().getLengthOfMergeChain() != NaiveAlgorithmForPathMergeVertex.kmerSize)
            		getRecordWriter().write(vertex.getVertexId(),vertex.getVertexValue());
            }
        }
}
