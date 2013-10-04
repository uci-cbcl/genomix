package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.pregelix.io.ScaffoldingVertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.BFSTraverseVertex.SearchInfo;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.Vertex;

public class ScaffoldingAggregator extends StatisticsAggregator<ScaffoldingVertexValueWritable> {

    public static HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> preScaffoldingMap = new HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>>();

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void step(Vertex<VKmer, ScaffoldingVertexValueWritable, NullWritable, MessageWritable> v) throws HyracksDataException {
        super.step(v);
        updateScaffoldingMap(v.getVertexValue().getScaffoldingMap());
    }

    @Override
    public void step(ScaffoldingVertexValueWritable partialResult) {
        super.step(partialResult);
        updateScaffoldingMap(partialResult.getScaffoldingMap());
    }

    public void updateScaffoldingMap(HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> otherMap) {
        HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> curMap = value.getScaffoldingMap();
        for (LongWritable readId : otherMap.keySet()) {
            if (curMap.containsKey(readId)) {
                curMap.get(readId).addAll(otherMap.get(readId));
            } else {
                curMap.put(readId, otherMap.get(readId));
            }
        }
    }

    @Override
    public ScaffoldingVertexValueWritable finishPartial() {
        return value;
    }

    @Override
    public ScaffoldingVertexValueWritable finishFinal() {
        updateAggregateState(preGlobalCounters);
        updateScaffoldingMap(preScaffoldingMap);
        return value;
    }

}
