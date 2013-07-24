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
    
    private NodeWritable outputNode = new NodeWritable();
    private NodeWritable tmpNode = new NodeWritable();
	@Override
	public void reduce(KmerBytesWritable key, Iterator<NodeWritable> values,
			OutputCollector<KmerBytesWritable, NodeWritable> output,
			Reporter reporter) throws IOException {
		outputNode.reset(0);
		
//		//copy first item to outputNode
//		if(values.hasNext()){
//		    NodeWritable tmpNode = values.next();
//		    outputNode.set(tmpNode);
//		}
		while (values.hasNext()) {
		    tmpNode.set(values.next());
		    outputNode.getNodeIdList().appendList(tmpNode.getNodeIdList());
		    outputNode.getFFList().appendList(tmpNode.getFFList());
		    outputNode.getFRList().appendList(tmpNode.getFRList());
		    outputNode.getRFList().appendList(tmpNode.getRFList());
		    outputNode.getRRList().appendList(tmpNode.getRRList());
		}
		output.collect(key,outputNode);
	}

}
