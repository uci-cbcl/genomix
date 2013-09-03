package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.pregelix.api.io.binary.GraphCleanVertexOutputFormat;
import edu.uci.ics.genomix.pregelix.io.P2VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P2ForPathMergeVertex;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;

public class P2PathMergeOutputFormat extends
    GraphCleanVertexOutputFormat<VKmerBytesWritable, P2VertexValueWritable, NullWritable> {

    @Override
    public VertexWriter<VKmerBytesWritable, P2VertexValueWritable, NullWritable> createVertexWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked")
        RecordWriter<VKmerBytesWritable, VertexValueWritable> recordWriter = binaryOutputFormat.getRecordWriter(context);
        return new BinaryLoadGraphVertexWriter(recordWriter);
    }

    /**
     * Simple VertexWriter that supports {@link BinaryLoadGraphVertex}
     */
    public static class BinaryLoadGraphVertexWriter extends
            BinaryVertexWriter<VKmerBytesWritable, P2VertexValueWritable, NullWritable> {
        public BinaryLoadGraphVertexWriter(RecordWriter<VKmerBytesWritable, VertexValueWritable> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VKmerBytesWritable, P2VertexValueWritable, NullWritable, ?> vertex)
                throws IOException, InterruptedException {
            byte selfFlag = (byte)(vertex.getVertexValue().getState() & State.VERTEX_MASK);
            if(!vertex.getVertexValue().isFakeVertex() && (vertex.getVertexValue().getState() == State.IS_HALT || selfFlag == State.IS_FINAL))
                getRecordWriter().write(vertex.getVertexId(), vertex.getVertexValue().get());
            P2ForPathMergeVertex.fakeVertexExist = false;
        }
    }
}
