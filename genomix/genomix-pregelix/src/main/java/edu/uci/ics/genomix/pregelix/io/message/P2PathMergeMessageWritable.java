package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class P2PathMergeMessageWritable extends PathMergeMessageWritable{
    
    public class P2MessageType{
        public static final byte FROM_PREDECESSOR = 0b1 << 1;
        public static final byte FROM_SUCCESSOR = 0b1 << 2;
    }
    
    private byte messageType; // otherwise, isFromSuccessor.
    
    public P2PathMergeMessageWritable(){
        super();
        messageType = 0;
    }
    
    public P2PathMergeMessageWritable(P2PathMergeMessageWritable msg){
        setSourceVertexId(msg.getSourceVertexId());
        setFlag(msg.getFlag());
        setNode(msg.getNode());
        setFlip(msg.isFlip());
        setUpdateMsg(msg.isUpdateMsg());
        messageType = msg.getMessageType();
    }
    
    public void reset(){
        super.reset();
        messageType = 0;
    }

    public byte getMessageType() {
        return messageType;
    }

    public void setMessageType(byte messageType) {
        this.messageType = messageType;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        messageType = in.readByte();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeByte(messageType);
    }
}
