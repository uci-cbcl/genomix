package edu.uci.ics.genomix.hadoop.contrailgraphbuilding;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;

@SuppressWarnings("deprecation")
public class GenomixReducer extends MapReduceBase implements
	Reducer<KmerBytesWritable, NodeWritable, KmerBytesWritable, NodeWritable>{

	@Override
	public void reduce(KmerBytesWritable key, Iterator<NodeWritable> values,
			OutputCollector<KmerBytesWritable, NodeWritable> output,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
