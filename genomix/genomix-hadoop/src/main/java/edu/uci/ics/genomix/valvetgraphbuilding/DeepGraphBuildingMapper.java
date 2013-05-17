package edu.uci.ics.genomix.valvetgraphbuilding;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;

@SuppressWarnings("deprecation")
public class DeepGraphBuildingMapper extends MapReduceBase implements
        Mapper<KmerBytesWritable, PositionListWritable, IntWritable, LineBasedmappingWritable> {
    IntWritable  numLine = new IntWritable();
    LineBasedmappingWritable lineBasedWriter = new LineBasedmappingWritable();
    @Override
    public void map(KmerBytesWritable key, PositionListWritable value, OutputCollector<IntWritable, LineBasedmappingWritable> output,
            Reporter reporter) throws IOException {
    }
}
