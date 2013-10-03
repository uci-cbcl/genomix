package edu.uci.ics.genomix.hadoop.contrailgraphbuilding;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.Node.EDGETYPE;
import edu.uci.ics.genomix.type.VKmer;

/**
 * GenomixReducer the 2nd step of graph building
 * 
 * @author anbangx
 */
@SuppressWarnings("deprecation")
public class GenomixReducer extends MapReduceBase implements Reducer<VKmer, Node, VKmer, Node> {

    private Node outputNode;
    private Node tmpNode;
    private float averageCoverage;

    @Override
    public void configure(JobConf job) {
        outputNode = new Node();
        tmpNode = new Node();
    }

    @Override
    public void reduce(VKmer key, Iterator<Node> values, OutputCollector<VKmer, Node> output, Reporter reporter)
            throws IOException {
        outputNode.reset();
        averageCoverage = 0;

        while (values.hasNext()) {
            tmpNode.setAsCopy(values.next());
            for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
                outputNode.getEdgeList(e).unionUpdate(tmpNode.getEdgeList(e));
            }
            outputNode.getStartReads().unionUpdate(tmpNode.getStartReads());
            outputNode.getEndReads().unionUpdate(tmpNode.getEndReads());
            averageCoverage += tmpNode.getAvgCoverage();
        }
        outputNode.setAvgCoverage(averageCoverage);
        output.collect(key, outputNode);
    }

}
