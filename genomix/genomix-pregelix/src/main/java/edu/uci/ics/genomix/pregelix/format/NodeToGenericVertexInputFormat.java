package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexInputFormat;
import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexInputFormat.BinaryVertexReader;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;

public abstract class NodeToGenericVertexInputFormat<V extends VertexValueWritable> extends
        BinaryVertexInputFormat<VKmer, V, NullWritable, MessageWritable> {
    /**
     * Format INPUT
     */
    @SuppressWarnings("unchecked")
    @Override
    public VertexReader<VKmer, V, NullWritable, MessageWritable> createVertexReader(InputSplit split,
            TaskAttemptContext context) throws IOException {
        return new BinaryDataCleanLoadGraphReader<V>(binaryInputFormat.createRecordReader(split, context));
    }
}

@SuppressWarnings("rawtypes")
class BinaryDataCleanLoadGraphReader<V extends VertexValueWritable> extends
        BinaryVertexReader<VKmer, V, NullWritable, MessageWritable> {
    private Vertex vertex;
    private VKmer vertexId = new VKmer();
    @SuppressWarnings("unchecked")
	private V vertexValue = (V)new VertexValueWritable();

    public BinaryDataCleanLoadGraphReader(RecordReader<VKmer, Node> recordReader) {
        super(recordReader);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return getRecordReader().nextKeyValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex<VKmer, V, NullWritable, MessageWritable> getCurrentVertex() throws IOException,
            InterruptedException {
        if (vertex == null)
            vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());

        vertex.getMsgList().clear();
        vertex.getEdges().clear();

        vertex.reset();
        if (getRecordReader() != null) {
            /**
             * set the src vertex id
             */
            vertexId.setAsCopy(getRecordReader().getCurrentKey());
            vertex.setVertexId(vertexId);
            /**
             * set the vertex value
             */
            vertexValue.setAsCopy(getRecordReader().getCurrentValue());
            vertex.setVertexValue(vertexValue);
        }

        return vertex;
    }
}
