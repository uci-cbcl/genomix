package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.api.io.binary.InitialGraphCleanVertexInputFormat;
import edu.uci.ics.genomix.pregelix.api.io.binary.InitialGraphCleanVertexInputFormat.BinaryVertexReader;
import edu.uci.ics.genomix.type.KmerBytesWritable;

public class InitialGraphCleanInputFormat extends
        InitialGraphCleanVertexInputFormat<KmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> {
    /**
     * Format INPUT
     */
    @SuppressWarnings("unchecked")
    @Override
    public VertexReader<KmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new BinaryLoadGraphReader(binaryInputFormat.createRecordReader(split, context));
    }
}

@SuppressWarnings("rawtypes")
class BinaryLoadGraphReader extends
        BinaryVertexReader<KmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> {
    
    private Vertex vertex;
    private KmerBytesWritable vertexId = new KmerBytesWritable();
    private NodeWritable node = new NodeWritable();
    private VertexValueWritable vertexValue = new VertexValueWritable();

    public BinaryLoadGraphReader(RecordReader<KmerBytesWritable, NodeWritable> recordReader) {
        super(recordReader);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return getRecordReader().nextKeyValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex<KmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> getCurrentVertex()
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
            node.set(getRecordReader().getCurrentValue());
            vertexValue.setKmerlength(node.getKmerLength());
            vertexValue.setNodeIdList(node.getNodeIdList());
            vertexValue.setFFList(node.getFFList());
            vertexValue.setFRList(node.getFRList());
            vertexValue.setRFList(node.getRFList());
            vertexValue.setRRList(node.getRRList());
            // TODO make this more efficient (don't use toString)
            vertexValue.setKmer(new VKmerBytesWritable(getRecordReader().getCurrentKey().toString()));
            vertexValue.setState(State.IS_NON);
            vertex.setVertexValue(vertexValue);
        }
    
        return vertex;
    }
}
