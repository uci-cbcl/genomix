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
    public PositionWritable tempVertex;
    public PositionListWritable listPosZeroInRead;
    public PositionListWritable listPosNonZeroInRead;
    public PositionListWritable tempPosList;
    public PositionListAndKmerWritable outputListAndKmer;
    @Override
    public void configure(JobConf job) {
        VertexID = new PositionWritable();
        tempVertex = new PositionWritable();
        listPosZeroInRead = new PositionListWritable();
        listPosNonZeroInRead = new PositionListWritable();
        tempPosList = new PositionListWritable();
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
            tempPosList.reset();
            for (int j = 0; j < listPosNonZeroInRead.getCountOfPosition(); j++) {
                tempVertex.set(listPosNonZeroInRead.getPosition(i));
                if(tempVertex.getReadID() != VertexID.getReadID()) {
                    int tempReadID = tempVertex.getReadID();
                    byte tempPosInRead = (byte) (tempVertex.getPosInRead() - 1);
                    tempVertex.set(tempReadID, tempPosInRead);
                    tempPosList.append(tempVertex);
                }
            }
            outputListAndKmer.set(tempPosList, key);
            output.collect(VertexID, outputListAndKmer);
        }
        for(int i = 0; i < listPosNonZeroInRead.getCountOfPosition(); i++) {
            VertexID.set(listPosNonZeroInRead.getPosition(i));
            tempPosList.reset();
            for (int j = 0; j < listPosZeroInRead.getCountOfPosition(); j++) {
                tempVertex.set(listPosNonZeroInRead.getPosition(i));
                if(tempVertex.getReadID() != VertexID.getReadID()) {
                    tempPosList.append(tempVertex);
                }
            }
            outputListAndKmer.set(tempPosList, key);
            output.collect(VertexID, outputListAndKmer);
        }
    }
}
