package edu.uci.ics.genomix.hadoop.velvetgraphbuilding;

//import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

public class ReadIDPartitioner implements Partitioner<PositionWritable, PositionListAndKmerWritable>{
    
    @Override
    public  int getPartition(PositionWritable key, PositionListAndKmerWritable value, int numPartitions){
        return (key.getReadID() & Integer.MAX_VALUE) % numPartitions;
    }

    @Override
    public void configure(JobConf arg0) {
    }
}
