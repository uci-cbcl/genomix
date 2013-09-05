package edu.uci.ics.genomix.pregelix.api.io.binary;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.io.VertexOutputFormat;
import edu.uci.ics.pregelix.api.io.VertexWriter;

/**
 * Abstract class that users should subclass to use their own text based vertex
 * output format.
 * 
 * @param <I>
 *            Vertex index value
 * @param <V>
 *            Vertex value
 * @param <E>
 *            Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class GraphCleanVertexOutputFormat<I extends WritableComparable, V extends Writable, E extends Writable>
        extends VertexOutputFormat<I, V, E> {
    /** Uses the SequenceFileOutputFormat to do everything */
    protected SequenceFileOutputFormat binaryOutputFormat = new SequenceFileOutputFormat();

    /**
     * Abstract class to be implemented by the user based on their specific
     * vertex output. Easiest to ignore the key value separator and only use key
     * instead.
     * 
     * @param <I>
     *            Vertex index value
     * @param <V>
     *            Vertex value
     * @param <E>
     *            Edge value
     */
    public static abstract class BinaryVertexWriter<I extends WritableComparable, V extends Writable, E extends Writable>
            implements VertexWriter<I, V, E> {
        /** Context passed to initialize */
        private TaskAttemptContext context;
        /** Internal line record writer */
        private final RecordWriter<VKmerBytesWritable, VertexValueWritable> lineRecordWriter;

        /**
         * Initialize with the LineRecordWriter.
         * 
         * @param lineRecordWriter
         *            Line record writer from SequenceFileOutputFormat
         */
        public BinaryVertexWriter(RecordWriter<VKmerBytesWritable, VertexValueWritable> lineRecordWriter) {
            this.lineRecordWriter = lineRecordWriter;
        }

        @Override
        public void initialize(TaskAttemptContext context) throws IOException {
            this.context = context;
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            lineRecordWriter.close(context);
        }

        /**
         * Get the line record writer.
         * 
         * @return Record writer to be used for writing.
         */
        public RecordWriter<VKmerBytesWritable, VertexValueWritable> getRecordWriter() {
            return lineRecordWriter;
        }

        /**
         * Get the context.
         * 
         * @return Context passed to initialize.
         */
        public TaskAttemptContext getContext() {
            return context;
        }
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        binaryOutputFormat.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return binaryOutputFormat.getOutputCommitter(context);
    }
}
