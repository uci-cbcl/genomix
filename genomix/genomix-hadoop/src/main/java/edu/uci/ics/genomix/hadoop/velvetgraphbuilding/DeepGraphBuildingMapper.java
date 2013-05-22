package edu.uci.ics.genomix.hadoop.velvetgraphbuilding;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

@SuppressWarnings("deprecation")
public class DeepGraphBuildingMapper extends MapReduceBase implements
        Mapper<KmerBytesWritable, PositionListWritable, PositionWritable, PositionListAndKmerWritable> {
    
    public PositionWritable VertexID;
    public PositionListWritable listPosZeroInRead;
    public PositionListWritable listPosNonZeroInRead;
    public PositionListAndKmerWritable outputListAndKmer;
    @Override
    public void configure(JobConf job) {
        VertexID = new PositionWritable();
        listPosZeroInRead = new PositionListWritable();
        listPosNonZeroInRead = new PositionListWritable();
        outputListAndKmer = new PositionListAndKmerWritable();
    }
    @Override
    public void map(KmerBytesWritable key, PositionListWritable value, OutputCollector<PositionWritable, PositionListAndKmerWritable> output,
            Reporter reporter) throws IOException {
        listPosZeroInRead.reset();
        listPosNonZeroInRead.reset();
        outputListAndKmer.reset();
        for(int i = 0; i < value.getLength(); i++) {
            VertexID.set(value.getPosition(i));
            if(VertexID.getPosInRead() == 0) {
                listPosZeroInRead.append(VertexID);
            }
            else {
                listPosNonZeroInRead.append(VertexID);
            }
        }
        for(int i = 0; i < listPosZeroInRead.getCountOfPosition(); i++) {
            VertexID.set(listPosZeroInRead.getPosition(i));
            outputListAndKmer.set(listPosNonZeroInRead, key);
            output.collect(VertexID, outputListAndKmer);
        }
        for(int i = 0; i < listPosNonZeroInRead.getCountOfPosition(); i++) {
            VertexID.set(listPosNonZeroInRead.getPosition(i));
            outputListAndKmer.set(listPosZeroInRead, key);
            output.collect(VertexID, outputListAndKmer);
        }
    }
}
