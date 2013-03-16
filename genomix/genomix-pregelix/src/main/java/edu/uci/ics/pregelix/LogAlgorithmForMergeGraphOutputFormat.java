package edu.uci.ics.pregelix;

import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;
import edu.uci.ics.pregelix.api.io.binary.BinaryVertexOutputFormat;
import edu.uci.ics.pregelix.example.io.ValueStateWritable;

public class LogAlgorithmForMergeGraphOutputFormat extends 
	BinaryVertexOutputFormat<BytesWritable, ValueStateWritable, NullWritable> {

        @Override
        public VertexWriter<BytesWritable, ValueStateWritable, NullWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<BytesWritable, ByteWritable> recordWriter = binaryOutputFormat.getRecordWriter(context);
            return new BinaryLoadGraphVertexWriter(recordWriter);
        }
        
        /**
         * Simple VertexWriter that supports {@link BinaryLoadGraphVertex}
         */
        public static class BinaryLoadGraphVertexWriter extends
                BinaryVertexWriter<BytesWritable, ValueStateWritable, NullWritable> {
            public BinaryLoadGraphVertexWriter(RecordWriter<BytesWritable, ByteWritable> lineRecordWriter) {
                super(lineRecordWriter);
            }

            @Override
            public void writeVertex(Vertex<BytesWritable, ValueStateWritable, NullWritable, ?> vertex) throws IOException,
                    InterruptedException {
                getRecordWriter().write(vertex.getVertexId(),
                		new ByteWritable(vertex.getVertexValue().getValue()));
            }
        }
}
