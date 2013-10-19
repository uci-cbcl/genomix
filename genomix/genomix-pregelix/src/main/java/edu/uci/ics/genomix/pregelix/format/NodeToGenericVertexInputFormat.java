package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;

import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexInputFormat;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;

public abstract class NodeToGenericVertexInputFormat<V extends VertexValueWritable> extends
        BinaryVertexInputFormat<VKmer, V, NullWritable, MessageWritable> {

    protected static class BinaryDataCleanLoadGraphReader<V extends VertexValueWritable> extends
            BinaryVertexReader<VKmer, V, NullWritable, MessageWritable> {
        private Vertex<VKmer, V, NullWritable, MessageWritable> vertex;
        private VKmer vertexId = new VKmer();
        protected V vertexValue;

        public BinaryDataCleanLoadGraphReader(RecordReader<VKmer, Node> recordReader) {
            super(recordReader);
        }

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        @Override
        public Vertex<VKmer, V, NullWritable, MessageWritable> getCurrentVertex() throws IOException,
                InterruptedException {
            if (vertex == null)
                vertex = BspUtils.createVertex(getContext().getConfiguration());

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
                if (vertexValue.getInternalKmer().getKmerLetterLength() == 0) // initial input directly from graph building
                    vertexValue.setInternalKmer(vertexId);
                vertex.setVertexValue(vertexValue);
            }

            return vertex;
        }
    }
}
