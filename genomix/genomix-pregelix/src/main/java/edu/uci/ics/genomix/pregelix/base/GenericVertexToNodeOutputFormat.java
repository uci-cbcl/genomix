package edu.uci.ics.genomix.pregelix.base;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.ReadHeadSet;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;

public abstract class GenericVertexToNodeOutputFormat<V extends Node> extends
        BinaryVertexOutputFormat<VKmer, V, NullWritable> {

    @Override
    public VertexWriter<VKmer, V, NullWritable> createVertexWriter(TaskAttemptContext context) throws IOException,
            InterruptedException {
        @SuppressWarnings("unchecked")
        RecordWriter<VKmer, Node> recordWriter = binaryOutputFormat.getRecordWriter(context);
        return new BinaryLoadGraphVertexWriter<V>(recordWriter);
    }

    /**
     * Simple VertexWriter that supports {@link BinaryLoadGraphVertex}
     */
    public static class BinaryLoadGraphVertexWriter<V extends Node> extends BinaryVertexWriter<VKmer, V, NullWritable> {
        private Node node;

        public BinaryLoadGraphVertexWriter(RecordWriter<VKmer, Node> lineRecordWriter) {
            super(lineRecordWriter);
            node = new Node();
            ReadHeadSet.forceWriteEntireBody(true);
        }

        @Override
        public void writeVertex(Vertex<VKmer, V, NullWritable, ?> vertex) throws IOException, InterruptedException {
            // keep only the values relevant to NodeToGenericVertexInputFormat
            node.setAsReference(vertex.getVertexValue());

            // HACK
            // if the kmer hasn't changed in length, it's equivalent to the vertexId and we don't need to write it
            if (node.getInternalKmer().getKmerLetterLength() == Kmer.getKmerLength()) {
                node.setInternalKmer(null);
            }
            getRecordWriter().write(vertex.getVertexId(), node);
        }
    }
}
