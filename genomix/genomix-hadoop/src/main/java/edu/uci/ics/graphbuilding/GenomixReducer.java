package edu.uci.ics.graphbuilding;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class GenomixReducer extends MapReduceBase implements
        Reducer<LongWritable, IntWritable, LongWritable, ValueWritable> {
    public void reduce(LongWritable key, Iterator<IntWritable> values,
            OutputCollector<LongWritable, ValueWritable> output, Reporter reporter) throws IOException {
        int groupByAdjList = 0;
        int count = 0;
        while (values.hasNext()) {
            //Merge By the all adjacent Nodes;
            groupByAdjList = groupByAdjList | values.next().get();
            count++;
        }
        output.collect(key, new ValueWritable(groupByAdjList, count));
    }
}
