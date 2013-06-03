package edu.uci.ics.genomix.hadoop.velvetgraphbuilding;

import java.io.IOException;
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

    private PositionWritable positionEntry;
    private PositionWritable tempVertex;
    private PositionListWritable listPosZeroInRead;
    private PositionListWritable listPosNonZeroInRead;
    private PositionListWritable tempPosList;
    private PositionListAndKmerWritable outputListAndKmer;
    private static int LAST_POSID;
    private static int KMER_SIZE;
    private static int READ_LENGTH;
    @Override
    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get("sizeKmer"));
        READ_LENGTH = Integer.parseInt(job.get("readLength"));
        positionEntry = new PositionWritable();
        tempVertex = new PositionWritable();
        listPosZeroInRead = new PositionListWritable();
        listPosNonZeroInRead = new PositionListWritable();
        tempPosList = new PositionListWritable();
        outputListAndKmer = new PositionListAndKmerWritable();
        LAST_POSID = READ_LENGTH - KMER_SIZE + 1;
    }

    private boolean isStart(byte posInRead) {
        return posInRead == 1 || posInRead == -LAST_POSID;
    }

    @Override
    public void map(KmerBytesWritable key, PositionListWritable value,
            OutputCollector<PositionWritable, PositionListAndKmerWritable> output, Reporter reporter)
            throws IOException {
        outputListAndKmer.reset();
        int tupleCount = value.getCountOfPosition();
        scanPosition(tupleCount, value);
        scanAgainAndOutput(listPosZeroInRead, listPosNonZeroInRead, key, output);
        scanAgainAndOutput(listPosNonZeroInRead, listPosZeroInRead, key, output);
    }

    public void scanPosition(int tupleCount, PositionListWritable value) {
        listPosZeroInRead.reset();
        listPosNonZeroInRead.reset();
        for (int i = 0; i < tupleCount; i++) {
            positionEntry.set(value.getPosition(i));
            if (isStart(positionEntry.getPosInRead())) {
                listPosZeroInRead.append(positionEntry);
            } else {
                listPosNonZeroInRead.append(positionEntry);
            }
        }
    }

    public void scanAgainAndOutput(PositionListWritable outputListInRead, PositionListWritable attriListInRead,
            KmerBytesWritable kmer, OutputCollector<PositionWritable, PositionListAndKmerWritable> output) throws IOException {
        for (int i = 0; i < outputListInRead.getCountOfPosition(); i++) {
            positionEntry.set(outputListInRead.getPosition(i));
            tempPosList.reset();
            for (int j = 0; j < attriListInRead.getCountOfPosition(); j++) {
                tempVertex.set(attriListInRead.getPosition(j));
                if (tempVertex.getReadID() != positionEntry.getReadID()) {
                    tempPosList.append(tempVertex);
                }
            }
            outputListAndKmer.set(tempPosList, kmer);
//            if(positionEntry.getReadID() == 1){
                output.collect(positionEntry, outputListAndKmer);
//            }
        }
    }
}
