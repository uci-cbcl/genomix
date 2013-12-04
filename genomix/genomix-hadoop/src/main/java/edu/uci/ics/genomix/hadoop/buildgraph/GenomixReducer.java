package edu.uci.ics.genomix.hadoop.buildgraph;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;

/**
 * GenomixReducer the 2nd step of graph building
 * 
 * @author anbangx
 */
@SuppressWarnings("deprecation")
public class GenomixReducer extends MapReduceBase implements Reducer<VKmer, Node, VKmer, Node> {

    private Node outputNode;

    @Override
    public void configure(JobConf job) {
        outputNode = new Node();
    }

    @Override
    public void reduce(VKmer key, Iterator<Node> values, OutputCollector<VKmer, Node> output, Reporter reporter)
            throws IOException {
        outputNode.reset();
        float averageCoverage = 0;

        Node curNode;
        while (values.hasNext()) {
            curNode = values.next();
            for (EDGETYPE e : EDGETYPE.values) {
                outputNode.getEdges(e).unionUpdate(curNode.getEdges(e));
            }
            outputNode.getUnflippedReadIds().addAll(curNode.getUnflippedReadIds());
            outputNode.getFlippedReadIds().addAll(curNode.getFlippedReadIds());
            averageCoverage += curNode.getAverageCoverage();
        }
        outputNode.setAverageCoverage(averageCoverage);

        output.collect(key, outputNode);
    }

}
