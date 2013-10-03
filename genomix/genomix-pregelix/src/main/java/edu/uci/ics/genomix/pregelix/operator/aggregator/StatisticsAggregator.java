package edu.uci.ics.genomix.pregelix.operator.aggregator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.Vertex;

/**
 * Global agrregator
 * 
 * @author anbangx
 */
public class StatisticsAggregator
        extends
        GlobalAggregator<VKmer, VertexValueWritable, NullWritable, MessageWritable, VertexValueWritable, VertexValueWritable> {

    public static HashMapWritable<ByteWritable, VLongWritable> preGlobalCounters = new HashMapWritable<ByteWritable, VLongWritable>();
    protected VertexValueWritable value = new VertexValueWritable();

    @Override
    public void init() {
        value.reset();
    }

    @Override
    public void step(Vertex<VKmer, VertexValueWritable, NullWritable, MessageWritable> v) throws HyracksDataException {
        HashMapWritable<ByteWritable, VLongWritable> counters = v.getVertexValue().getCounters();
        updateAggregateState(counters);
    }

    @Override
    public void step(VertexValueWritable partialResult) {
        HashMapWritable<ByteWritable, VLongWritable> counters = partialResult.getCounters();
        updateAggregateState(counters);
    }

    public void updateAggregateState(HashMapWritable<ByteWritable, VLongWritable> counters) {
        for (ByteWritable counterName : counters.keySet()) {
            if (value.getCounters().containsKey(counterName)) {
                VLongWritable counterVal = value.getCounters().get(counterName);
                value.getCounters().get(counterName).set(counterVal.get() + counters.get(counterName).get());
            } else {
                value.getCounters().put(counterName, counters.get(counterName));
            }
        }
    }

    @Override
    public VertexValueWritable finishPartial() {
        return value;
    }

    @Override
    public VertexValueWritable finishFinal() {
        updateAggregateState(preGlobalCounters);
        return value;
    }

}
