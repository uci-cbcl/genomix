package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.pregelix.api.io.binary.GraphCleanVertexOutputFormat;
import edu.uci.ics.genomix.pregelix.io.P2VertexValue;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;

public class P2PathMergeOutputFormat extends
    GraphCleanVertexOutputFormat<VKmer, P2VertexValue, NullWritable> {

    @Override
    public VertexWriter<VKmer, P2VertexValue, NullWritable> createVertexWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked")
        RecordWriter<VKmer, VertexValueWritable> recordWriter = binaryOutputFormat.getRecordWriter(context);
        return new BinaryLoadGraphVertexWriter(recordWriter);
    }

    /**
     * Simple VertexWriter that supports {@link BinaryLoadGraphVertex}
     */
    public static class BinaryLoadGraphVertexWriter extends
            BinaryVertexWriter<VKmer, P2VertexValue, NullWritable> {
        public BinaryLoadGraphVertexWriter(RecordWriter<VKmer, VertexValueWritable> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VKmer, P2VertexValue, NullWritable, ?> vertex)
                throws IOException, InterruptedException {
//            byte selfFlag = (byte)(vertex.getVertexValue().getState() & State.VERTEX_MASK);
//            if(!vertex.getVertexValue().isFakeVertex() && (vertex.getVertexValue().getState() == State.IS_HALT || selfFlag == State.IS_FINAL))
//                getRecordWriter().write(vertex.getVertexId(), vertex.getVertexValue().get());
//            synchronized(BasicGraphCleanVertex.lock){
//                BasicGraphCleanVertex.fakeVertexExist = false;
//                BasicGraphCleanVertex.fakeVertex = null;
//            }
        }
    }
}
