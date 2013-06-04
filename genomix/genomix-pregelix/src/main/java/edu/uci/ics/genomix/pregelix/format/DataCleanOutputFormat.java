package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryVertexOutputFormat;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;

public class DataCleanOutputFormat extends
        BinaryVertexOutputFormat<PositionWritable, ValueStateWritable, NullWritable> {

    @Override
    public VertexWriter<PositionWritable, ValueStateWritable, NullWritable> createVertexWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked")
        RecordWriter<PositionWritable, ValueStateWritable> recordWriter = binaryOutputFormat.getRecordWriter(context);
        return new BinaryLoadGraphVertexWriter(recordWriter);
    }

    /**
     * Simple VertexWriter that supports {@link BinaryLoadGraphVertex}
     */
    public static class BinaryLoadGraphVertexWriter extends
            BinaryVertexWriter<PositionWritable, ValueStateWritable, NullWritable> {
        public BinaryLoadGraphVertexWriter(RecordWriter<PositionWritable, ValueStateWritable> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<PositionWritable, ValueStateWritable, NullWritable, ?> vertex)
                throws IOException, InterruptedException {
            getRecordWriter().write(vertex.getVertexId(), vertex.getVertexValue());
        }
    }
}
