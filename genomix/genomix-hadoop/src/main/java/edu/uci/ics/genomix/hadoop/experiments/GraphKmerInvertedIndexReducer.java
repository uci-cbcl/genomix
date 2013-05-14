package edu.uci.ics.genomix.hadoop.experiments;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.experiment.KmerVertexID;
import edu.uci.ics.genomix.experiment.VertexIDList;
import edu.uci.ics.genomix.experiment.VertexIDListWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

@SuppressWarnings({ "deprecation", "unused" })
public class GraphKmerInvertedIndexReducer extends MapReduceBase implements
        Reducer<KmerBytesWritable, KmerVertexID, KmerBytesWritable, VertexIDListWritable> {
    VertexIDList vertexList = new VertexIDList(1);
    VertexIDListWritable listWritable = new VertexIDListWritable();
    @Override
    public void reduce(KmerBytesWritable key, Iterator<KmerVertexID> values,
            OutputCollector<KmerBytesWritable, VertexIDListWritable> output, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            KmerVertexID vertexID = values.next();
            vertexList.addELementToList(vertexID.getReadID(), vertexID.getPosInRead());
        }
        listWritable.set(vertexList);
        output.collect(key, listWritable);
    }
}
