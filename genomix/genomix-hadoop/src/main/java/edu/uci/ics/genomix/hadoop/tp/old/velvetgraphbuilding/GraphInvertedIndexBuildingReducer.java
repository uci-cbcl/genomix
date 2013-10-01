package edu.uci.ics.genomix.hadoop.tp.old.velvetgraphbuilding;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

//import edu.uci.ics.genomix.hadoop.tp.oldtype.position.PositionListWritable;
//import edu.uci.ics.genomix.hadoop.tp.oldtype.position.PositionWritable;
//import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.hadoop.tp.old.type.velvet.*;

@SuppressWarnings({ "deprecation", "unused" })
public class GraphInvertedIndexBuildingReducer extends MapReduceBase implements
        Reducer<KmerBytesWritable, PositionWritable, KmerBytesWritable, PositionListWritable> {
    PositionListWritable outputlist = new PositionListWritable();
    @Override
    public void reduce(KmerBytesWritable key, Iterator<PositionWritable> values,
            OutputCollector<KmerBytesWritable, PositionListWritable> output, Reporter reporter) throws IOException {
        outputlist.reset();
        while (values.hasNext()) {
            outputlist.append(values.next());
        }
        output.collect(key, outputlist);
    }
}
