package edu.uci.ics.pregelix;

import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.io.text.TextVertexInputFormat;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.example.io.MessageWritable;

public class TextLoadGraphInputFormat extends
		TextVertexInputFormat<BytesWritable, ByteWritable, NullWritable, MessageWritable>{
	
	/**
	 * Format INPUT
	 */
    @Override
    public VertexReader<BytesWritable, ByteWritable, NullWritable, MessageWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new TextLoadGraphReader(textInputFormat.createRecordReader(split, context));
    }
    
    @SuppressWarnings("rawtypes")
    class TextLoadGraphReader extends
            TextVertexReader<BytesWritable, ByteWritable, NullWritable, MessageWritable> {
        private final static String separator = " ";
        private Vertex vertex;
        private BytesWritable vertexId = new BytesWritable();
        private ByteWritable vertexValue = new ByteWritable();

        public TextLoadGraphReader(RecordReader<LongWritable, Text> lineRecordReader) {
            super(lineRecordReader);
        }

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Vertex<BytesWritable, ByteWritable, NullWritable, MessageWritable> getCurrentVertex() throws IOException,
                InterruptedException {
            if (vertex == null)
                vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());

            vertex.getMsgList().clear();
            vertex.getEdges().clear();
            Text line = getRecordReader().getCurrentValue();
            String[] fields = line.toString().split(separator);

            if (fields.length > 0) {
                /**
                 * set the src vertex id
                 */
            	BytesWritable src = new BytesWritable(fields[0].getBytes());
                vertexId.set(src);
                vertex.setVertexId(vertexId);

                
                /**
                 * set the vertex value
                 */
                byte[] temp = fields[1].getBytes();
                vertexValue.set(temp[0]);
                vertex.setVertexValue(vertexValue);
                
            }
            return vertex;
        }
    }

}
