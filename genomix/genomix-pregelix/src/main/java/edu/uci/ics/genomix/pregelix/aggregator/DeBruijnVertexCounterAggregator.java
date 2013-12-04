package edu.uci.ics.genomix.pregelix.aggregator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;

import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.vertex.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.HadoopCountersAggregator;

/**
 * Aggregator that saves the hadoop Counters object from a DeBruijnGraphVertex
 * 
 * @author anbangx
 */
public class DeBruijnVertexCounterAggregator extends
        HadoopCountersAggregator<VKmer, VertexValueWritable, NullWritable, MessageWritable, Counters> {
    @Override
    public void step(Vertex<VKmer, VertexValueWritable, NullWritable, MessageWritable> v) throws HyracksDataException {
        DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable> vertex = (DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable>) v;
        getCounters().incrAllCounters(vertex.getCounters());
    }
}
