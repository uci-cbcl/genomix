package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.pregelix.io.P2VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.api.io.binary.InitialGraphCleanVertexInputFormat;
import edu.uci.ics.genomix.pregelix.api.io.binary.InitialGraphCleanVertexInputFormat.BinaryVertexReader;

public class P2InitialGraphCleanInputFormat extends
        InitialGraphCleanVertexInputFormat<VKmer, P2VertexValueWritable, NullWritable, MessageWritable> {
    /**
     * Format INPUT
     */
    @SuppressWarnings("unchecked")
    @Override
    public VertexReader<VKmer, P2VertexValueWritable, NullWritable, MessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new P2BinaryLoadGraphReader(binaryInputFormat.createRecordReader(split, context));
    }
}

@SuppressWarnings("rawtypes")
class P2BinaryLoadGraphReader extends
        BinaryVertexReader<VKmer, P2VertexValueWritable, NullWritable, MessageWritable> {
    
    private Vertex vertex;
    private VKmer vertexId = new VKmer();
    private Node node = new Node();
    private P2VertexValueWritable vertexValue = new P2VertexValueWritable();

    public P2BinaryLoadGraphReader(RecordReader<VKmer, Node> recordReader) {
        super(recordReader);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return getRecordReader().nextKeyValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex<VKmer, P2VertexValueWritable, NullWritable, MessageWritable> getCurrentVertex()
            throws IOException, InterruptedException {
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
            node.setAsCopy(getRecordReader().getCurrentValue());
            vertexValue.setNode(node);
            vertexValue.setInternalKmer(vertexId);
            vertexValue.setState(State.IS_NON);
            vertex.setVertexValue(vertexValue);
        }
    
        return vertex;
    }
}
