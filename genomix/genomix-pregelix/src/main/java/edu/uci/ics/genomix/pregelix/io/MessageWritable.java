package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.type.CheckMessage;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionWritable;

public class MessageWritable implements WritableComparable<MessageWritable> {
    /**
     * sourceVertexId stores source vertexId when headVertex sends the message
     * stores neighber vertexValue when pathVertex sends the message
     * file stores the point to the file that stores the chains of connected DNA
     */
    private PositionWritable sourceVertexId;
    private KmerBytesWritable kmer;
    private AdjacencyListWritable neighberNode; //incoming or outgoing
    private byte flag;

    private byte checkMessage;

    public MessageWritable() {
        sourceVertexId = new PositionWritable();
        kmer = new KmerBytesWritable(0);
        neighberNode = new AdjacencyListWritable();
        flag = Message.NON;
        checkMessage = (byte) 0;
    }
    
    public void set(MessageWritable msg) {
        checkMessage = 0;
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.set(msg.getSourceVertexId());
        }
        if (kmer != null) {
            checkMessage |= CheckMessage.CHAIN;
            this.kmer.set(msg.getKmer());
        }
        if (neighberNode != null) {
            checkMessage |= CheckMessage.NEIGHBER;
            this.neighberNode.set(msg.getNeighberNode());
        }
        checkMessage |= CheckMessage.ADJMSG;
        this.flag = msg.getFlag();
    }

    public void set(PositionWritable sourceVertexId, KmerBytesWritable chainVertexId, AdjacencyListWritable neighberNode, byte message) {
        checkMessage = 0;
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.set(sourceVertexId.getReadID(),sourceVertexId.getPosInRead());
        }
        if (chainVertexId != null) {
            checkMessage |= CheckMessage.CHAIN;
            this.kmer.set(chainVertexId);
        }
        if (neighberNode != null) {
            checkMessage |= CheckMessage.NEIGHBER;
            this.neighberNode.set(neighberNode);
        }
        this.flag = message;
    }

    public void reset() {
        checkMessage = 0;
        kmer.reset(1);
        neighberNode.reset();
        flag = Message.NON;
    }

    public PositionWritable getSourceVertexId() {
        return sourceVertexId;
    }

    public void setSourceVertexId(PositionWritable sourceVertexId) {
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.set(sourceVertexId.getReadID(),sourceVertexId.getPosInRead());
        }
    }
    
    public KmerBytesWritable getKmer() {
        return kmer;
    }

    public void setChainVertexId(KmerBytesWritable chainVertexId) {
        if (chainVertexId != null) {
            checkMessage |= CheckMessage.CHAIN;
            this.kmer.set(chainVertexId);
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

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(checkMessage);
        if ((checkMessage & CheckMessage.SOURCE) != 0)
            sourceVertexId.write(out);
        if ((checkMessage & CheckMessage.CHAIN) != 0)
            kmer.write(out);
        if ((checkMessage & CheckMessage.NEIGHBER) != 0)
            neighberNode.write(out);
        out.write(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.reset();
        checkMessage = in.readByte();
        if ((checkMessage & CheckMessage.SOURCE) != 0)
            sourceVertexId.readFields(in);
        if ((checkMessage & CheckMessage.CHAIN) != 0)
            kmer.readFields(in);
        if ((checkMessage & CheckMessage.NEIGHBER) != 0)
            neighberNode.readFields(in);
        flag = in.readByte();
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
