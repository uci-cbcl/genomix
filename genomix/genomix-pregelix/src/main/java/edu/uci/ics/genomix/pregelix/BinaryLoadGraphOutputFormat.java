package edu.uci.ics.genomix.pregelix;

import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexOutputFormat;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;

public class BinaryLoadGraphOutputFormat extends 
	BinaryVertexOutputFormat<BytesWritable, ByteWritable, NullWritable> {

        @Override
        public VertexWriter<BytesWritable, ByteWritable, NullWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<BytesWritable, ByteWritable> recordWriter = binaryOutputFormat.getRecordWriter(context);
            return new BinaryLoadGraphVertexWriter(recordWriter);
        }
        
        /**
         * Simple VertexWriter that supports {@link BinaryLoadGraphVertex}
         */
        public static class BinaryLoadGraphVertexWriter extends
                BinaryVertexWriter<BytesWritable, ByteWritable, NullWritable> {
            public BinaryLoadGraphVertexWriter(RecordWriter<BytesWritable, ByteWritable> lineRecordWriter) {
                super(lineRecordWriter);
            }

            @Override
            public void writeVertex(Vertex<BytesWritable, ByteWritable, NullWritable, ?> vertex) throws IOException,
                    InterruptedException {
                getRecordWriter().write(vertex.getVertexId(),vertex.getVertexValue());
            }
        }
}
