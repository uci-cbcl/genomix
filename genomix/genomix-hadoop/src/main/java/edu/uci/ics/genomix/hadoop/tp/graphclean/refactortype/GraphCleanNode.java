package edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;

import org.apache.hadoop.mapred.OutputCollector;

import edu.uci.ics.genomix.hadoop.tp.graphclean.type.MessageFlag;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;

public class GraphCleanNode extends NodeWritable {

    private static final long serialVersionUID = 1L;

    protected VKmerBytesWritable repeatKmer;
    protected EDGETYPE repeatEdgetype; //for detect tandemRepeat
    protected Iterator<VKmerBytesWritable> kmerIterator;

    public GraphCleanNode() {
        repeatKmer = new VKmerBytesWritable();
//        repeatDir = 0b00 << 0;
    }
    

    /**
     * check if it is a tandem repeat
     */
    public  boolean isTandemRepeat(GraphCleanDestId VertexId, GraphCleanNode value){
        VKmerBytesWritable kmerToCheck;
        for(EDGETYPE et : EnumSet.allOf(EDGETYPE.class)){
            Iterator<VKmerBytesWritable> it = value.getEdgeList(et).getKeyIterator();
            while(it.hasNext()){
                kmerToCheck = it.next();
                if(kmerToCheck.equals(VertexId)){
                    repeatEdgetype = et;
                    repeatKmer.setAsCopy(kmerToCheck);
                    return true;
                }
            }
        }
        return false;
    }

//    public void processTandemRepeatForPathMerge() {
//        getEdgeList(repeatDir).remove(repeatKmer);
//        while (isTandemRepeat())
//            getEdgeList(repeatDir).remove(repeatKmer);
//    }
    
//    public void sendBasicMsgToNeighbors(NodeWritable nodeToProcessd, DIR dir, byte outFlag,
//            GraphCleanDestId key, MsgListWritable msgList,GraphCleanGenericValue outputValue,
//            OutputCollector<GraphCleanDestId, GraphCleanGenericValue> output) throws IOException {
//        switch (dir) {
//            case NEXT:
//                sendBaicMsgToForward(nodeToProcessd, outFlag, key, msgList, outputValue, output);
//                break;
//            case PREVIOUS:
//                sendBaicMsgToBackward(nodeToProcessd, outFlag, key, msgList, outputValue, output);
//                break;
//            case ALL:
//                sendBaicMsgToForward(nodeToProcessd, outFlag, key, msgList, outputValue, output);
//                sendBaicMsgToBackward(nodeToProcessd, outFlag, key, msgList, outputValue, output);
//                break;
//        }
//    }
//
//
//    public void sendBaicMsgToForward(NodeWritable nodeToProcessd, byte outFlag,
//            GraphCleanDestId key, MsgListWritable msgList,GraphCleanGenericValue outputValue,
//            OutputCollector<GraphCleanDestId, GraphCleanGenericValue> output) throws IOException {
//        kmerIterator = nodeToProcessd.getEdgeList(DirectionFlag.DIR_FF).getKeys(); // FFList
//        while (kmerIterator.hasNext()) {
//            msgList.get(0).setFlag(MessageFlag.DIR_FF);
//            msgList.get(0).setSourceVertexId(getInternalKmer());
//            key.setAsCopy(kmerIterator.next());
////            sendMsg(key, msgList, output);
//            outputValue.set(msgList);
//            sendMsg(key, outputValue, output);
//        }
//        kmerIterator = nodeToProcessd.getEdgeList(DirectionFlag.DIR_FR).getKeys(); // FRList
//        while (kmerIterator.hasNext()) {
//            msgList.get(0).setFlag(MessageFlag.DIR_FR);
//            msgList.get(0).setSourceVertexId(getInternalKmer());
//            key.setAsCopy(kmerIterator.next());
//            outputValue.set(msgList);
//            sendMsg(key, outputValue, output);
//        }
//    }
//    
//    public void sendBaicMsgToBackward(NodeWritable nodeToProcessd, byte outFlag,
//            GraphCleanDestId key, MsgListWritable msgList,GraphCleanGenericValue outputValue,
//            OutputCollector<GraphCleanDestId, GraphCleanGenericValue> output) throws IOException {
//        kmerIterator = nodeToProcessd.getEdgeList(DirectionFlag.DIR_RR).getKeys(); // FFList
//        while (kmerIterator.hasNext()) {
//            msgList.get(0).setFlag(MessageFlag.DIR_RR);
//            msgList.get(0).setSourceVertexId(getInternalKmer());
//            key.setAsCopy(kmerIterator.next());
//            outputValue.set(msgList);
//            sendMsg(key, outputValue, output);
//        }
//        kmerIterator = nodeToProcessd.getEdgeList(DirectionFlag.DIR_RF).getKeys(); // FRList
//        while (kmerIterator.hasNext()) {
//            msgList.get(0).setFlag(MessageFlag.DIR_RF);
//            msgList.get(0).setSourceVertexId(getInternalKmer());
//            key.setAsCopy(kmerIterator.next());
//            outputValue.set(msgList);
//            sendMsg(key, outputValue, output);
//        }
//    }
    
    public void setAsCopy(NodeWritable other) {
        super.setAsCopy(other);
    }


    public VKmerBytesWritable getRepeatKmer() {
        return repeatKmer;
    }
}
