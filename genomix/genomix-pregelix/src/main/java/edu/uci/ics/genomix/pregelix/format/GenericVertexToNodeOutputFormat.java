package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexOutputFormat;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;

public abstract class GenericVertexToNodeOutputFormat<V extends Node> 
	extends BinaryVertexOutputFormat<VKmer, V, NullWritable> {

    @Override
    public VertexWriter<VKmer, V, NullWritable> createVertexWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        @SuppressWarnings("unchecked")
        RecordWriter<VKmer, Node> recordWriter = binaryOutputFormat.getRecordWriter(context);
        return new BinaryLoadGraphVertexWriter<V>(recordWriter);
    }

    /**
     * Simple VertexWriter that supports {@link BinaryLoadGraphVertex}
     */
    public static class BinaryLoadGraphVertexWriter<V extends Node> extends
            BinaryVertexWriter<VKmer, V, NullWritable> {
        public BinaryLoadGraphVertexWriter(RecordWriter<VKmer, Node> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VKmer, V, NullWritable, ?> vertex) throws IOException,
                InterruptedException {
            getRecordWriter().write(vertex.getVertexId(), vertex.getVertexValue().getNode());
        }
    }
}
