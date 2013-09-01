package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.api.io.binary.GraphCleanVertexInputFormat;
import edu.uci.ics.genomix.pregelix.api.io.binary.GraphCleanVertexInputFormat.BinaryDataCleanVertexReader;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class GraphCleanInputFormat extends
    GraphCleanVertexInputFormat<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> {
    /**
     * Format INPUT
     */
    @SuppressWarnings("unchecked")
    @Override
    public VertexReader<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new BinaryDataCleanLoadGraphReader(binaryInputFormat.createRecordReader(split, context));
    }
}

@SuppressWarnings("rawtypes")
class BinaryDataCleanLoadGraphReader extends
    BinaryDataCleanVertexReader<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> {
    private Vertex vertex;
    private VKmerBytesWritable vertexId = new VKmerBytesWritable();
    private VertexValueWritable vertexValue = new VertexValueWritable();

    public BinaryDataCleanLoadGraphReader(RecordReader<VKmerBytesWritable, VertexValueWritable> recordReader) {
        super(recordReader);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return getRecordReader().nextKeyValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> getCurrentVertex()
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
            vertexValue.setAsCopy(getRecordReader().getCurrentValue());
            vertex.setVertexValue(vertexValue);
        }

        return vertex;
    }
}
