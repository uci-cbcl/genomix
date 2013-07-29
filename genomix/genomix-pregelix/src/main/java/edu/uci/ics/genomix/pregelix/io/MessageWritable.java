package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.type.CheckMessage;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.type.KmerBytesWritable;

public class MessageWritable implements WritableComparable<MessageWritable> {
    /**
     * sourceVertexId stores source vertexId when headVertex sends the message
     * stores neighber vertexValue when pathVertex sends the message
     * file stores the point to the file that stores the chains of connected DNA
     */
    private KmerBytesWritable sourceVertexId;
    private KmerBytesWritable kmer;
    private AdjacencyListWritable neighberNode; //incoming or outgoing
    private byte flag;
    private boolean isFlip;
    private int kmerlength = 0;
    private boolean updateMsg = false;

    private byte checkMessage;

    public MessageWritable() {
        sourceVertexId = new KmerBytesWritable();
        kmer = new KmerBytesWritable();
        neighberNode = new AdjacencyListWritable();
        flag = Message.NON;
        isFlip = false;
        checkMessage = (byte) 0;
    }
    
    public MessageWritable(int kmerSize) {
        kmerlength = kmerSize;
        sourceVertexId = new KmerBytesWritable();
        kmer = new KmerBytesWritable();
        neighberNode = new AdjacencyListWritable(kmerSize);
        flag = Message.NON;
        isFlip = false;
        checkMessage = (byte) 0;
    }
    
    public void set(MessageWritable msg) {
        this.kmerlength = msg.kmerlength;
        checkMessage = 0;
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.setAsCopy(msg.getSourceVertexId());
        }
        if (kmer != null) {
            checkMessage |= CheckMessage.CHAIN;
            this.kmer.setAsCopy(msg.getKmer());
        }
        if (neighberNode != null) {
            checkMessage |= CheckMessage.NEIGHBER;
            this.neighberNode.set(msg.getNeighberNode());
        }
        checkMessage |= CheckMessage.ADJMSG;
        this.flag = msg.getFlag();
        updateMsg = msg.isUpdateMsg();
    }

    public void set(int kmerlength, KmerBytesWritable sourceVertexId, KmerBytesWritable chainVertexId, AdjacencyListWritable neighberNode, byte message) {
        this.kmerlength = kmerlength;
        checkMessage = 0;
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.setAsCopy(sourceVertexId);
        }
        if (chainVertexId != null) {
            checkMessage |= CheckMessage.CHAIN;
            this.kmer.setAsCopy(chainVertexId);
        }
        if (neighberNode != null) {
            checkMessage |= CheckMessage.NEIGHBER;
            this.neighberNode.set(neighberNode);
        }
        this.flag = message;
    }

    public void reset() {
        reset(0);
    }
    
    public void reset(int kmerSize) {
        checkMessage = (byte) 0;
        kmerlength = kmerSize;
//        kmer.reset();
        neighberNode.reset(kmerSize);
        flag = Message.NON;
        isFlip = false;
    }

    public KmerBytesWritable getSourceVertexId() {
        return sourceVertexId;
    }

    public void setSourceVertexId(KmerBytesWritable sourceVertexId) {
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.setAsCopy(sourceVertexId);
        }
    }
    
    public KmerBytesWritable getKmer() {
        return kmer;
    }

    public void setChainVertexId(KmerBytesWritable chainVertexId) {
        if (chainVertexId != null) {
            checkMessage |= CheckMessage.CHAIN;
            this.kmer.setAsCopy(chainVertexId);
        }
    }
    
    public AdjacencyListWritable getNeighberNode() {
        return neighberNode;
    }

    public void setNeighberNode(AdjacencyListWritable neighberNode) {
        if(neighberNode != null){
            checkMessage |= CheckMessage.NEIGHBER;
            this.neighberNode.set(neighberNode);
        }
    }
    
    public int getLengthOfChain() {
        return kmer.getKmerLength();
    }

    public byte getFlag() {
        return flag;
    }

    public void setFlag(byte message) {
        this.flag = message;
    }
    
    public boolean isFlip() {
        return isFlip;
    }

    public void setFlip(boolean isFlip) {
        this.isFlip = isFlip;
    }

    
    public boolean isUpdateMsg() {
        return updateMsg;
    }

    public void setUpdateMsg(boolean updateMsg) {
        this.updateMsg = updateMsg;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(kmerlength);
        out.writeByte(checkMessage);
        if ((checkMessage & CheckMessage.SOURCE) != 0)
            sourceVertexId.write(out);
        if ((checkMessage & CheckMessage.CHAIN) != 0)
            kmer.write(out);
        if ((checkMessage & CheckMessage.NEIGHBER) != 0)
            neighberNode.write(out);
        out.writeBoolean(isFlip);
        out.writeByte(flag); 
        out.writeBoolean(updateMsg);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        kmerlength = in.readInt();
        this.reset(kmerlength);
        checkMessage = in.readByte();
        if ((checkMessage & CheckMessage.SOURCE) != 0)
            sourceVertexId.readFields(in);
        if ((checkMessage & CheckMessage.CHAIN) != 0)
            kmer.readFields(in);
        if ((checkMessage & CheckMessage.NEIGHBER) != 0)
            neighberNode.readFields(in);
        isFlip = in.readBoolean();
        flag = in.readByte();
        updateMsg = in.readBoolean();
    }

    @Override
    public int hashCode() {
        return sourceVertexId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MessageWritable) {
            MessageWritable tp = (MessageWritable) o;
            return sourceVertexId.equals(tp.sourceVertexId);
        }
        return false;
    }

    @Override
    public String toString() {
        return sourceVertexId.toString();
    }

    @Override
    public int compareTo(MessageWritable tp) {
        return sourceVertexId.compareTo(tp.sourceVertexId);
    }
}
