package edu.uci.ics.pregelix;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat.TextVertexWriter;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.io.MessageWritable;

/*
 * vertexId: BytesWritable
 * vertexValue: ByteWritable
 * edgeValue: NullWritable
 * message: MessageWritable
 * 
 * DNA:
 * A: 00
 * C: 01
 * G: 10
 * T: 11
 * 
 * succeed node
 *  A 00000001 1
 *  G 00000010 2
 *  C 00000100 4
 *  T 00001000 8
 * precursor node
 *  A 00010000 16
 *  G 00100000 32
 *  C 01000000 64
 *  T 10000000 128
 *  
 * For example, ONE LINE in input file: 00,01,10	0001,0010,
 * That means that vertexId is ACG, its succeed node is A and its precursor node is C.
 * The succeed node and precursor node will be stored in vertexValue and we don't use edgeValue.
 * The details about message are in edu.uci.ics.pregelix.example.io.MessageWritable. 
 */
public class LoadGraphVertex extends Vertex<BytesWritable, ByteWritable, NullWritable, MessageWritable>{
	
	private ByteWritable tmpVertexValue = new ByteWritable();
	
	/**
	 * For test, in compute method, make each vertexValue shift 1 to left.
	 * It will be modified when going forward to next step.
	 */
	@Override
	public void compute(Iterator<MessageWritable> msgIterator) {
		if(getSuperstep() == 1){
			tmpVertexValue.set(getVertexValue().get());
			tmpVertexValue.set((byte) (tmpVertexValue.get() << 1));
			setVertexValue(tmpVertexValue);
		}
		else
			voteToHalt();
	 }
	
    /**
     * Simple VertexWriter that supports {@link SimpleLoadGraphVertex}
     */
    public static class SimpleLoadGraphVertexWriter extends
            TextVertexWriter<BytesWritable, ByteWritable, NullWritable> {
        public SimpleLoadGraphVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<BytesWritable, ByteWritable, NullWritable, ?> vertex) throws IOException,
                InterruptedException {
            getRecordWriter().write(new Text(vertex.getVertexId().toString()),
                    new Text(vertex.getVertexValue().toString()));
        }
    }

    /**
     * Simple VertexOutputFormat that supports {@link SimpleLoadGraphVertex}
     */
    public static class SimpleLoadGraphVertexOutputFormat extends
            TextVertexOutputFormat<BytesWritable, ByteWritable, NullWritable> {

        @Override
        public VertexWriter<BytesWritable, ByteWritable, NullWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter = textOutputFormat.getRecordWriter(context);
            return new SimpleLoadGraphVertexWriter(recordWriter);
        }
    }
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(LoadGraphVertex.class.getSimpleName());
        job.setVertexClass(LoadGraphVertex.class);
        /**
         * TextInput and TextOutput
         */ 
          job.setVertexInputFormatClass(TextLoadGraphInputFormat.class);
          job.setVertexOutputFormatClass(SimpleLoadGraphVertexOutputFormat.class); 
        
        
        /**
         * BinaryInput and BinaryOutput
         */
      /*  job.setVertexInputFormatClass(BinaryLoadGraphInputFormat.class); 
        job.setVertexOutputFormatClass(BinaryLoadGraphOutputFormat.class); 
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(ByteWritable.class);*/
        Client.run(args, job);
	}
}
