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
public class BasicAggregator<V extends VertexValueWritable> extends
        GlobalAggregator<VKmer, V, NullWritable, MessageWritable, HashMapWritable<ByteWritable, VLongWritable>, HashMapWritable<ByteWritable, VLongWritable>> {

//    public HashMapWritable<ByteWritable, VLongWritable> preGlobalCounters = new HashMapWritable<ByteWritable, VLongWritable>();
//    protected V value = (V) new VertexValueWritable(); 
    private HashMapWritable<ByteWritable, VLongWritable> globalCounters = new HashMapWritable<ByteWritable, VLongWritable>();
    
    @Override
    public void init() {
//        for(ByteWritable counterName : globalCounters.keySet()){
//            globalCounters.get(counterName).set(0);
//        }
    }

    @Override
    public void step(Vertex<VKmer, V, NullWritable, MessageWritable> v) throws HyracksDataException {
        HashMapWritable<ByteWritable, VLongWritable> vertexCounters = v.getVertexValue().getCounters();
        updateAggregateState(vertexCounters);
    }

    @Override
    public void step(HashMapWritable<ByteWritable, VLongWritable> partialResult) {
        updateAggregateState(partialResult);
    }

    public void updateAggregateState(HashMapWritable<ByteWritable, VLongWritable> counters) {
        for (ByteWritable counterName : counters.keySet()) {
            if (globalCounters.containsKey(counterName)) {
                VLongWritable counterVal = globalCounters.get(counterName);
                globalCounters.get(counterName).set(counterVal.get() + counters.get(counterName).get());
            } else {
                globalCounters.put(counterName, counters.get(counterName));
            }
        }
    }

    @Override
    public HashMapWritable<ByteWritable, VLongWritable> finishPartial() {
        return globalCounters;
    }

    @Override
    public HashMapWritable<ByteWritable, VLongWritable> finishFinal() {
//        updateAggregateState(preGlobalCounters);
        return globalCounters;
    }

}
