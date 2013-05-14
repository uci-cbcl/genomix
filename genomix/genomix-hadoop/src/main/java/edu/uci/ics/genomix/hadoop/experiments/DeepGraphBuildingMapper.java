package edu.uci.ics.genomix.hadoop.experiments;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.experiment.VertexIDList;
import edu.uci.ics.genomix.experiment.VertexIDListWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;

@SuppressWarnings("deprecation")
public class DeepGraphBuildingMapper extends MapReduceBase implements
        Mapper<KmerBytesWritable, VertexIDListWritable, IntWritable, LineBasedmappingWritable> {
    IntWritable  numLine = new IntWritable();
    VertexIDList vertexList = new VertexIDList(1);
    LineBasedmappingWritable lineBasedWriter = new LineBasedmappingWritable();
    @Override
    public void map(KmerBytesWritable key, VertexIDListWritable value, OutputCollector<IntWritable, LineBasedmappingWritable> output,
            Reporter reporter) throws IOException {
        vertexList.set(value.get());
        for(int i = 0; i < vertexList.getUsedSize(); i++) {
            numLine.set(vertexList.getReadListElement(i));
            lineBasedWriter.set(i, value, key);
            output.collect(numLine, lineBasedWriter);
        }
    }
}
