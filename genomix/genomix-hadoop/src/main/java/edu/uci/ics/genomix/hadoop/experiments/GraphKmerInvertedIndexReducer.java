package edu.uci.ics.genomix.hadoop.experiments;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.experiment.Position;
import edu.uci.ics.genomix.experiment.PositionList;
import edu.uci.ics.genomix.experiment.VertexIDListWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

@SuppressWarnings({ "deprecation", "unused" })
public class GraphKmerInvertedIndexReducer extends MapReduceBase implements
        Reducer<KmerBytesWritable, Position, KmerBytesWritable, VertexIDListWritable> {
    PositionList vertexList = new PositionList(1);
    VertexIDListWritable listWritable = new VertexIDListWritable();
    @Override
    public void reduce(KmerBytesWritable key, Iterator<Position> values,
            OutputCollector<KmerBytesWritable, VertexIDListWritable> output, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            Position vertexID = values.next();
            vertexList.addELementToList(vertexID.getReadID(), vertexID.getPosInRead());
        }
        listWritable.set(vertexList);
        output.collect(key, listWritable);
    }
}
