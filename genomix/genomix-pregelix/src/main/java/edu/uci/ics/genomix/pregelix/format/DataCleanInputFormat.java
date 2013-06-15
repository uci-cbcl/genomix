package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryDataCleanVertexInputFormat;
import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryDataCleanVertexInputFormat.BinaryDataCleanVertexReader;

public class DataCleanInputFormat extends
    BinaryDataCleanVertexInputFormat<PositionWritable, ValueStateWritable, NullWritable, MessageWritable> {
    /**
     * Format INPUT
     */
    @SuppressWarnings("unchecked")
    @Override
    public VertexReader<PositionWritable, ValueStateWritable, NullWritable, MessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new BinaryDataCleanLoadGraphReader(binaryInputFormat.createRecordReader(split, context));
    }
}

@SuppressWarnings("rawtypes")
class BinaryDataCleanLoadGraphReader extends
    BinaryDataCleanVertexReader<PositionWritable, ValueStateWritable, NullWritable, MessageWritable> {
    private Vertex vertex;
    private PositionWritable vertexId = new PositionWritable();
    private ValueStateWritable vertexValue = new ValueStateWritable();

    public BinaryDataCleanLoadGraphReader(RecordReader<PositionWritable, ValueStateWritable> recordReader) {
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

        vertex.reset();
        if (getRecordReader() != null) {
            /**
             * set the src vertex id
             */
            vertexId.set(getRecordReader().getCurrentKey());
            vertex.setVertexId(vertexId);
            /**
             * set the vertex value
             */
            vertexValue.set(getRecordReader().getCurrentValue());
            vertex.setVertexValue(vertexValue);
        }

        return vertex;
    }
}
