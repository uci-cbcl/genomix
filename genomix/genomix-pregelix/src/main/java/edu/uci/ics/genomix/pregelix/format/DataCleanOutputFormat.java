package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.pregelix.api.io.binary.BinaryDataCleanVertexOutputFormat;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;

public class DataCleanOutputFormat extends
    BinaryDataCleanVertexOutputFormat<PositionWritable, VertexValueWritable, NullWritable> {

    @Override
    public VertexWriter<PositionWritable, VertexValueWritable, NullWritable> createVertexWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked")
        RecordWriter<PositionWritable, VertexValueWritable> recordWriter = binaryOutputFormat.getRecordWriter(context);
        return new BinaryLoadGraphVertexWriter(recordWriter);
    }

    /**
     * Simple VertexWriter that supports {@link BinaryLoadGraphVertex}
     */
    public static class BinaryLoadGraphVertexWriter extends
            BinaryVertexWriter<PositionWritable, VertexValueWritable, NullWritable> {
        public BinaryLoadGraphVertexWriter(RecordWriter<PositionWritable, VertexValueWritable> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<PositionWritable, VertexValueWritable, NullWritable, ?> vertex)
                throws IOException, InterruptedException {
            //if(vertex.getVertexValue().getState() != MessageFlag.IS_OLDHEAD)
            getRecordWriter().write(vertex.getVertexId(), vertex.getVertexValue());
        }
    }
}
