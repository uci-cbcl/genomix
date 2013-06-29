package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexInputFormat;
import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexInputFormat.BinaryVertexReader;

public class NaiveAlgorithmForPathMergeInputFormat extends
        BinaryVertexInputFormat<PositionWritable, VertexValueWritable, NullWritable, MessageWritable> {
    /**
     * Format INPUT
     */
    @SuppressWarnings("unchecked")
    @Override
    public VertexReader<PositionWritable, VertexValueWritable, NullWritable, MessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new BinaryLoadGraphReader(binaryInputFormat.createRecordReader(split, context));
    }
}

@SuppressWarnings("rawtypes")
class BinaryLoadGraphReader extends
        BinaryVertexReader<PositionWritable, VertexValueWritable, NullWritable, MessageWritable> {
    private Vertex vertex;
    private NodeWritable node = new NodeWritable();
    private PositionWritable vertexId = new PositionWritable();
    private VertexValueWritable vertexValue = new VertexValueWritable();

    public BinaryLoadGraphReader(RecordReader<NodeWritable, NullWritable> recordReader) {
        super(recordReader);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return getRecordReader().nextKeyValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex<PositionWritable, VertexValueWritable, NullWritable, MessageWritable> getCurrentVertex()
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
            node = getRecordReader().getCurrentKey();
            vertexId.set(node.getNodeID());
            vertex.setVertexId(vertexId);
            /**
             * set the vertex value
             */
            vertexValue.setFFList(node.getFFList());
            vertexValue.setFRList(node.getFRList());
            vertexValue.setRFList(node.getRFList());
            vertexValue.setRRList(node.getRRList());
            vertexValue.setKmer(node.getKmer());
            vertexValue.setState(State.NON_VERTEX);
            vertex.setVertexValue(vertexValue);
        }

        return vertex;
    }
}
