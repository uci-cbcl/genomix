package edu.uci.ics.genomix.hadoop.tp.graphclean.pmcommon;

import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;

public class PathMergeBasicNodeWritable extends NodeWritable{
    
    protected VKmerBytesWritable repeatKmer = null; //for detect tandemRepeat
    protected byte repeatDir; //for detect tandemRepeat
    
    /**
     * check if it is a tandem repeat
     */
    public boolean isTandemRepeat(){
        for(byte d : DirectionFlag.values){
            Iterator<VKmerBytesWritable> it = getEdgeList(d).getKeys();
            while(it.hasNext()){
                repeatKmer.setAsCopy(it.next());
                if(repeatKmer.equals(getInternalKmer())){
                    repeatDir = d; //TODO ?
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * initiate head, rear and path node
     */
    public void initState(Iterator<M> msgIterator) {
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if(isHaltNode())
                voteToHalt();
            else if(getHeadFlag() != MessageFlag.IS_HEAD && !isTandemRepeat()){
                if(isValidPath()){
                    setHeadMergeDir();
                    activate();
                } else{
                    getVertexValue().setState(MessageFlag.IS_HALT);
                    voteToHalt();
                }
            } else if(getHeadFlagAndMergeDir() == getMsgFlagAndMergeDir()){
                activate();
            } else{ /** already set up **/
                /** if headMergeDir are not the same **/
                getVertexValue().setState(MessageFlag.IS_HALT);
                voteToHalt();
            }
        }
    }
}
