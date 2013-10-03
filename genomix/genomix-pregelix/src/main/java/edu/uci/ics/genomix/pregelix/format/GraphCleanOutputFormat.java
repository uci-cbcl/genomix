package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.pregelix.api.io.binary.GraphCleanVertexOutputFormat;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;

public class GraphCleanOutputFormat extends GraphCleanVertexOutputFormat<VKmer, VertexValueWritable, NullWritable> {

    @Override
    public VertexWriter<VKmer, VertexValueWritable, NullWritable> createVertexWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        @SuppressWarnings("unchecked")
        RecordWriter<VKmer, VertexValueWritable> recordWriter = binaryOutputFormat.getRecordWriter(context);
        return new BinaryLoadGraphVertexWriter(recordWriter);
    }

    /**
     * Simple VertexWriter that supports {@link BinaryLoadGraphVertex}
     */
    public static class BinaryLoadGraphVertexWriter extends
            BinaryVertexWriter<VKmer, VertexValueWritable, NullWritable> {
        public BinaryLoadGraphVertexWriter(RecordWriter<VKmer, VertexValueWritable> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VKmer, VertexValueWritable, NullWritable, ?> vertex) throws IOException,
                InterruptedException {
            getRecordWriter().write(vertex.getVertexId(), vertex.getVertexValue());
        }
    }
}
