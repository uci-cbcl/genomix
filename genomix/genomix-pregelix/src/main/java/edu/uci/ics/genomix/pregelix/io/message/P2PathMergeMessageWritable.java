package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.pregelix.io.KmerAndDirWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.type.VKmer;

public class P2PathMergeMessageWritable extends PathMergeMessageWritable{
    
    public class P2MessageType{
        public static final byte FROM_PREDECESSOR = 0b1 << 1;
        public static final byte FROM_SUCCESSOR = 0b1 << 2;
    }
    
    private byte messageType; // otherwise, isFromSuccessor.
    
    private HashMapWritable<VKmer, KmerAndDirWritable> apexMap; //<apexId, deleteKmerAndDir>
    private boolean isUpdateApexEdges;
    
    public P2PathMergeMessageWritable(){
        super();
        messageType = 0;
        apexMap = new HashMapWritable<VKmer, KmerAndDirWritable>();
        isUpdateApexEdges = false;
    }
    
    public P2PathMergeMessageWritable(P2PathMergeMessageWritable msg){
        setSourceVertexId(msg.getSourceVertexId());
        setFlag(msg.getFlag());
        setNode(msg.getNode());
        setFlip(msg.isFlip());
        setUpdateMsg(msg.isUpdateMsg());
        messageType = msg.getMessageType();
        setApexMap(msg.getApexMap());
        isUpdateApexEdges = msg.isUpdateApexEdges;
    }
    
    public void reset(){
        super.reset();
        messageType = 0;
//        apexMap.clear();
        isUpdateApexEdges = false;
    }

    public byte getMessageType() {
        return messageType;
    }

    public void setMessageType(byte messageType) {
        this.messageType = messageType;
    }

    public HashMapWritable<VKmer, KmerAndDirWritable> getApexMap() {
        return apexMap;
    }

    public void setApexMap(HashMapWritable<VKmer, KmerAndDirWritable> apexMap) {
        this.apexMap = new HashMapWritable<VKmer, KmerAndDirWritable>(apexMap);
    }
    
    public boolean isUpdateApexEdges() {
        return isUpdateApexEdges;
    }

    public void setUpdateApexEdges(boolean isUpdateApexEdges) {
        this.isUpdateApexEdges = isUpdateApexEdges;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        messageType = in.readByte();
        apexMap.readFields(in);
        isUpdateApexEdges = in.readBoolean();
    }
    

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeByte(messageType);
        apexMap.write(out);
        out.writeBoolean(isUpdateApexEdges);
    }
}
