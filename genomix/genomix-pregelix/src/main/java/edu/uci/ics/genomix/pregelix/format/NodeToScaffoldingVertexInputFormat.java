package edu.uci.ics.genomix.pregelix.format;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.pregelix.io.ScaffoldingVertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.io.VertexReader;

public class NodeToScaffoldingVertexInputFormat extends NodeToGenericVertexInputFormat<ScaffoldingVertexValueWritable> {

    @Override
    public VertexReader<VKmer, ScaffoldingVertexValueWritable, NullWritable, MessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new BinaryDataCleanLoadGraphReaderScaffolding(binaryInputFormat.createRecordReader(split, context));
    }

    private class BinaryDataCleanLoadGraphReaderScaffolding extends
            NodeToGenericVertexInputFormat.BinaryDataCleanLoadGraphReader<ScaffoldingVertexValueWritable> {

        public BinaryDataCleanLoadGraphReaderScaffolding(RecordReader<VKmer, Node> recordReader) {
            super(recordReader);
            vertexValue = new ScaffoldingVertexValueWritable();
        }
    }
}
