package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.io.VertexReader;

public class NodeToVertexInputFormat extends NodeToGenericVertexInputFormat<VertexValueWritable> {

    @Override
    public VertexReader<VKmer, VertexValueWritable, NullWritable, MessageWritable> createVertexReader(InputSplit split,
            TaskAttemptContext context) throws IOException {
        return new BinaryDataCleanLoadGraphReaderVertexValue(binaryInputFormat.createRecordReader(split, context));
    }

    private class BinaryDataCleanLoadGraphReaderVertexValue extends
            NodeToGenericVertexInputFormat.BinaryDataCleanLoadGraphReader<VertexValueWritable> {

        public BinaryDataCleanLoadGraphReaderVertexValue(RecordReader<VKmer, Node> recordReader) {
            super(recordReader);
            vertexValue = new VertexValueWritable();
        }
    }
}
