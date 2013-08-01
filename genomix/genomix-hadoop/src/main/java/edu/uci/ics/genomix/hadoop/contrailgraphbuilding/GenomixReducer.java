package edu.uci.ics.genomix.hadoop.contrailgraphbuilding;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;

@SuppressWarnings("deprecation")
public class GenomixReducer extends MapReduceBase implements
	Reducer<KmerBytesWritable, NodeWritable, KmerBytesWritable, NodeWritable>{
    
    public static int KMER_SIZE;
    private NodeWritable outputNode;
    private NodeWritable tmpNode;
    
    @Override
    public void configure(JobConf job) {
        KMER_SIZE = GenomixMapper.KMER_SIZE;
        KmerBytesWritable.setGlobalKmerLength(KMER_SIZE);
        outputNode = new NodeWritable();
        tmpNode = new NodeWritable();
    }
    
	@Override
	public void reduce(KmerBytesWritable key, Iterator<NodeWritable> values,
			OutputCollector<KmerBytesWritable, NodeWritable> output,
			Reporter reporter) throws IOException {
		outputNode.reset();
		
		while (values.hasNext()) {
		    tmpNode.set(values.next());
		    outputNode.getNodeIdList().appendList(tmpNode.getNodeIdList());
		    outputNode.getFFList().appendList(tmpNode.getFFList()); //appendList need to check if insert node exists
		    outputNode.getFRList().appendList(tmpNode.getFRList());
		    outputNode.getRFList().appendList(tmpNode.getRFList());
		    outputNode.getRRList().appendList(tmpNode.getRRList());
		}
		output.collect(key,outputNode);
	}

}
