package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexInputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class LogAlgorithmForPathMergeInputFormat extends
        BinaryVertexInputFormat<PositionWritable, ValueStateWritable, NullWritable, MessageWritable> {
    /**
     * Format INPUT
     */
    @SuppressWarnings("unchecked")
    @Override
    public VertexReader<PositionWritable, ValueStateWritable, NullWritable, MessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new BinaryLoadGraphReader(binaryInputFormat.createRecordReader(split, context));
    }

    @SuppressWarnings("rawtypes")
    class BinaryLoadGraphReader extends
            BinaryVertexReader<PositionWritable, ValueStateWritable, NullWritable, MessageWritable> {
        private Vertex vertex = null;
        private NodeWritable node = new NodeWritable();
        private PositionWritable vertexId = new PositionWritable(); 
        private ValueStateWritable vertexValue = new ValueStateWritable();

        public BinaryLoadGraphReader(RecordReader<NodeWritable, NullWritable> recordReader) {
            super(recordReader);
        }

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Vertex<PositionWritable, ValueStateWritable, NullWritable, MessageWritable> getCurrentVertex()
                throws IOException, InterruptedException {
            if (vertex == null)
                vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());

            vertex.getMsgList().clear();
            vertex.getEdges().clear();

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
                vertexValue.setIncomingList(node.getIncomingList());
                vertexValue.setOutgoingList(node.getOutgoingList());
                vertexValue.setMergeChain(node.getKmer());
                vertexValue.setState(State.NON_VERTEX);
                vertex.setVertexValue(vertexValue);
            }

            return vertex;
        }
    }

}
