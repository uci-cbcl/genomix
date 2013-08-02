package edu.uci.ics.genomix.hadoop.contrailgraphbuilding;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

@SuppressWarnings("deprecation")
public class GenomixReducer extends MapReduceBase implements
	Reducer<VKmerBytesWritable, NodeWritable, VKmerBytesWritable, NodeWritable>{
    
    public static int KMER_SIZE;
    private NodeWritable outputNode;
    private NodeWritable tmpNode;
    
    @Override
    public void configure(JobConf job) {
        KMER_SIZE = GenomixMapper.KMER_SIZE;
        outputNode = new NodeWritable();
        tmpNode = new NodeWritable();
    }
    
	@Override
	public void reduce(VKmerBytesWritable key, Iterator<NodeWritable> values,
			OutputCollector<VKmerBytesWritable, NodeWritable> output,
			Reporter reporter) throws IOException {
		outputNode.reset();
		
		while (values.hasNext()) {
		    tmpNode.set(values.next());
		    outputNode.getNodeIdList().appendList(tmpNode.getNodeIdList());
		    outputNode.getFFList().unionUpdate(tmpNode.getFFList()); //appendList need to check if insert node exists
		    outputNode.getFRList().unionUpdate(tmpNode.getFRList());
		    outputNode.getRFList().unionUpdate(tmpNode.getRFList());
		    outputNode.getRRList().unionUpdate(tmpNode.getRRList());
		}
		output.collect(key,outputNode);
	}

}
