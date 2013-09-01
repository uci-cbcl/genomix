package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.pregelix.io.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.KmerListAndFlagListWritable;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VLongWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.Vertex;

public class ScaffoldingAggregator extends
    StatisticsAggregator{
    
    public static HashMapWritable<VLongWritable, KmerListAndFlagListWritable> preScaffoldingMap = new HashMapWritable<VLongWritable, KmerListAndFlagListWritable>();
    
    @Override
    public void init() {
        super.init();
    }

    @Override
    public void step(Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> v)
            throws HyracksDataException {
        super.step(v);
        updateScaffoldingMap(v.getVertexValue().getScaffoldingMap());
    }

    @Override
    public void step(VertexValueWritable partialResult) {
        super.step(partialResult);
        updateScaffoldingMap(partialResult.getScaffoldingMap());
    }
    
    public void updateScaffoldingMap(HashMapWritable<VLongWritable, KmerListAndFlagListWritable> otherMap){
        HashMapWritable<VLongWritable, KmerListAndFlagListWritable> curMap = value.getScaffoldingMap();
        for(VLongWritable readId : otherMap.keySet()){
            if(curMap.containsKey(readId)){
                curMap.get(readId).add(otherMap.get(readId));
            } else{
                curMap.put(readId, otherMap.get(readId));
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
        updateScaffoldingMap(preScaffoldingMap);
        return value;
    }

}
