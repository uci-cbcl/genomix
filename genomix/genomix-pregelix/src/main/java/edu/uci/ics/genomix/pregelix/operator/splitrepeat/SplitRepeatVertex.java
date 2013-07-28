package edu.uci.ics.genomix.pregelix.operator.splitrepeat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.BasicGraphCleanVertex;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;

public class SplitRepeatVertex extends 
    BasicGraphCleanVertex{

    Map<KmerBytesWritable, PositionListWritable> kmerMap = new HashMap<KmerBytesWritable, PositionListWritable>();
    String[][] connectedTable = new String[][]{
            {"FF", "RF"},
            {"FF", "RR"},
            {"FR", "RF"},
            {"FR", "RR"}
    };
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        if(getSuperstep() == 1){
            if(getVertexValue().getDegree() > 2){
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsgToAllNeighborNodes(getVertexValue());
            }
            voteToHalt();
        } else if(getSuperstep() == 2){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                outgoingMsg.setNodeIdList(getVertexValue().getNodeIdList());
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg);
            }
            voteToHalt();
        } else if(getSuperstep() == 3){
            kmerMap.clear();
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                kmerMap.put(incomingMsg.getSourceVertexId(), incomingMsg.getNodeIdList());
            }
            /** process connectedTable **/
            for(int i = 0; i < 4; i++){
                getVertexValue().getFFList();
            }
        }
    }
}
