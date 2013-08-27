package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.pregelix.io.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VLongWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.ScaffoldingVertex.KmerListAndFlagListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.Vertex;

public class ScaffoldingAggregator extends
    StatisticsAggregator{
    
    public static HashMapWritable<VLongWritable, KmerListAndFlagListWritable> preScaffoldingMap = new HashMapWritable<VLongWritable, KmerListAndFlagListWritable>();
    
    private VKmerListWritable kmerList = new VKmerListWritable();
    private ArrayListWritable<BooleanWritable> flagList = new ArrayListWritable<BooleanWritable>();
    private KmerListAndFlagListWritable kmerListAndflagList = new KmerListAndFlagListWritable();
    private HashMapWritable<VLongWritable, KmerListAndFlagListWritable> scaffoldingMap = new HashMapWritable<VLongWritable, KmerListAndFlagListWritable>();
    
    @Override
    public void init() {
        super.init();
    }

    @Override
    public void step(Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> v)
            throws HyracksDataException {
        super.step(v);
        scaffoldingMap.clear();
        addStartReadsToScaffoldingMap(v.getVertexId(), v.getVertexValue());
        addEndReadsToScaffoldingMap(v.getVertexId(), v.getVertexValue());
        updateScaffoldingMap(scaffoldingMap);
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
    
    public void addStartReadsToScaffoldingMap(VKmerBytesWritable id, VertexValueWritable value){
        boolean isflip = false;
        for(PositionWritable pos : value.getStartReads()){
            long readId = pos.getReadId();
            if(scaffoldingMap.containsKey(readId)){
                kmerList.setCopy(scaffoldingMap.get(readId).getKmerList());
                kmerList.append(id);
                flagList.clear();
                flagList.addAll(scaffoldingMap.get(readId).getFlagList());
                flagList.add(new BooleanWritable(isflip));
            } else{
                kmerList.reset();
                kmerList.append(id);
                flagList.clear();
                flagList.add(new BooleanWritable(isflip));
            }
            kmerListAndflagList.setKmerList(kmerList);
            kmerListAndflagList.setFlagList(flagList);
            scaffoldingMap.put(new VLongWritable(readId), kmerListAndflagList);
        }
    }
    
    public void addEndReadsToScaffoldingMap(VKmerBytesWritable id, VertexValueWritable value){
        boolean isflip = true;
        for(PositionWritable pos : value.getEndReads()){
            long readId = pos.getReadId();
            if(scaffoldingMap.containsKey(readId)){
                kmerList.setCopy(scaffoldingMap.get(readId).getKmerList());
                kmerList.append(id);
                flagList.clear();
                flagList.addAll(scaffoldingMap.get(readId).getFlagList());
                flagList.add(new BooleanWritable(isflip));
            } else{
                kmerList.reset();
                kmerList.append(id);
                flagList.clear();
                flagList.add(new BooleanWritable(isflip));
            }
            kmerListAndflagList.setKmerList(kmerList);
            kmerListAndflagList.setFlagList(flagList);
            scaffoldingMap.put(new VLongWritable(readId), kmerListAndflagList);
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
