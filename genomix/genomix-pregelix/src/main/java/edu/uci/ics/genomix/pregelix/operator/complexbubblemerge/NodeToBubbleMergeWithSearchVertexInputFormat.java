package edu.uci.ics.genomix.pregelix.operator.complexbubblemerge;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.base.NodeToGenericVertexInputFormat;
import edu.uci.ics.pregelix.api.io.VertexReader;

public class NodeToBubbleMergeWithSearchVertexInputFormat extends
        NodeToGenericVertexInputFormat<BubbleMergeWithSearchVertexValueWritable> {

    @Override
    public VertexReader<VKmer, BubbleMergeWithSearchVertexValueWritable, NullWritable, MessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new BinaryDataCleanLoadGraphReaderScaffolding(binaryInputFormat.createRecordReader(split, context));
    }

    private class BinaryDataCleanLoadGraphReaderScaffolding extends
            NodeToGenericVertexInputFormat.BinaryDataCleanLoadGraphReader<BubbleMergeWithSearchVertexValueWritable> {

        public BinaryDataCleanLoadGraphReaderScaffolding(RecordReader<VKmer, Node> recordReader) {
            super(recordReader);
            vertexValue = new BubbleMergeWithSearchVertexValueWritable();
        }
    }
}
