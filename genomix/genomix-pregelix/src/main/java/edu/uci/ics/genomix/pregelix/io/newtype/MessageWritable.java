package edu.uci.ics.genomix.pregelix.io.newtype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.type.CheckMessage;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.pregelix.api.io.WritableSizable;

public class MessageWritable extends NodeWritable
    implements WritableSizable {

    private static final long serialVersionUID = 1L;

    private VKmerBytesWritable sourceVertexId; // store srcNode id
    private byte flag; // store message type
    private boolean isFlip; // use for path merge
    private boolean updateMsg = false; // use for distinguish updateMsg or mergeMsg
    private VKmerBytesWritable startVertexId; //use for MergeBubble
    private VKmerListWritable pathList; //use for BFSTravese
    private boolean srcFlip = false; //use for BFSTravese
    private boolean destFlip = false; //use for BFSTravese
    
    private byte checkMessage;

    public MessageWritable() {
        sourceVertexId = new VKmerBytesWritable();
        internalKmer = new VKmerBytesWritable();
        neighberNode = new AdjacencyListWritable();
        startVertexId = new VKmerBytesWritable();
        averageCoverage = 0;
        flag = Message.NON;
        isFlip = false;
        checkMessage = (byte) 0;
        pathList = new VKmerListWritable();
    }
    
    public MessageWritable(int kmerSize) {
        kmerlength = kmerSize;
        sourceVertexId = new VKmerBytesWritable(kmerSize);
        internalKmer = new VKmerBytesWritable(0);

        neighberNode = new AdjacencyListWritable(kmerSize);
        startVertexId = new VKmerBytesWritable(kmerSize);
        averageCoverage = 0;
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
        if (internalKmer != null) {
            checkMessage |= CheckMessage.INTERNALKMER;
            this.internalKmer.setAsCopy(msg.getInternalKmer());

        }
        if (neighberNode != null) {
            checkMessage |= CheckMessage.NEIGHBER;
            this.neighberNode.set(msg.getNeighberNode());
        }
        if (startVertexId != null) {
            checkMessage |= CheckMessage.START;
            this.startVertexId.setAsCopy(msg.getStartVertexId());
        }
        this.flag = msg.getFlag();
        updateMsg = msg.isUpdateMsg();
    }

    public void set(int kmerlength, VKmerBytesWritable sourceVertexId, VKmerBytesWritable chainVertexId, AdjacencyListWritable neighberNode, byte message) {
        this.kmerlength = kmerlength;
        checkMessage = 0;
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.setAsCopy(sourceVertexId);
        }
        if (chainVertexId != null) {
            checkMessage |= CheckMessage.INTERNALKMER;
            this.internalKmer.setAsCopy(chainVertexId);

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
//        internalKmer.reset();
        neighberNode.reset(kmerSize);
        startVertexId.reset(kmerSize);
        averageCoverage = 0;
        flag = Message.NON;
        isFlip = false;
    }

    public VKmerBytesWritable getSourceVertexId() {
        return sourceVertexId;
    }

    public void setSourceVertexId(VKmerBytesWritable sourceVertexId) {
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.setAsCopy(sourceVertexId);
        }
    }
    
    public VKmerBytesWritable getInternalKmer() {
        return internalKmer;
    }

    public void setInternalKmer(VKmerBytesWritable internalKmer) {
        if (internalKmer != null) {
            checkMessage |= CheckMessage.INTERNALKMER;
            this.internalKmer.setAsCopy(internalKmer);
        }
    }
    
    // use for scaffolding
    public VKmerBytesWritable getMiddleVertexId() {
        return internalKmer;
    }

    public void setMiddleVertexId(VKmerBytesWritable middleKmer) {
        if (middleKmer != null) {
            checkMessage |= CheckMessage.INTERNALKMER;
            this.internalKmer.setAsCopy(middleKmer);
        }
    }
    
    public VKmerBytesWritable getCreatedVertexId() {
        return internalKmer;
    }

    public void setCreatedVertexId(VKmerBytesWritable actualKmer) {
        if (actualKmer != null) {
            checkMessage |= CheckMessage.INTERNALKMER;
            this.internalKmer.setAsCopy(actualKmer);
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
    
    public VKmerBytesWritable getStartVertexId() {
        return startVertexId;
    }

    public void setStartVertexId(VKmerBytesWritable startVertexId) {
        if(startVertexId != null){
            checkMessage |= CheckMessage.START;
            this.startVertexId.setAsCopy(startVertexId);
        }
    }
    
    /** for Scaffolding, startVertexId is used as seekedVertexId **/  
    public VKmerBytesWritable getSeekedVertexId() {
        return startVertexId;
    }

    public void setSeekedVertexId(VKmerBytesWritable startVertexId) {
        if(startVertexId != null){
            checkMessage |= CheckMessage.START;
            this.startVertexId.setAsCopy(startVertexId);
        }
    }
    
    public float getAverageCoverage() {
        return averageCoverage;
    }

    public void setAverageCoverage(float averageCoverage) {
        this.averageCoverage = averageCoverage;
    }

    public int getLengthOfChain() {
        return internalKmer.getKmerLetterLength();
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
    
    public boolean isEven() {
        return isFlip;
    }

    public void setEven(boolean isEven) {
        this.isFlip = isEven;
    }

    
    public boolean isUpdateMsg() {
        return updateMsg;
    }

    public void setUpdateMsg(boolean updateMsg) {
        this.updateMsg = updateMsg;
    }

    public PositionListWritable getNodeIdList() {
        return nodeIdList;
    }

    public void setNodeIdList(PositionListWritable nodeIdList) {
        if(nodeIdList != null){
            checkMessage |= CheckMessage.NODEIDLIST;
            this.nodeIdList.set(nodeIdList);
        }
    }

    public VKmerListWritable getPathList() {
        return pathList;
    }

    public void setPathList(VKmerListWritable pathList) {
        if(pathList != null){
            checkMessage |= CheckMessage.PATHLIST;
            this.pathList.setCopy(pathList);
        }
    }

    public boolean isSrcFlip() {
        return srcFlip;
    }

    public void setSrcFlip(boolean srcFlip) {
        this.srcFlip = srcFlip;
    }

    public boolean isDestFlip() {
        return destFlip;
    }

    public void setDestFlip(boolean destFlip) {
        this.destFlip = destFlip;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(kmerlength);
        out.writeByte(checkMessage);
        if ((checkMessage & CheckMessage.SOURCE) != 0)
            sourceVertexId.write(out);
        if ((checkMessage & CheckMessage.INTERNALKMER) != 0)
            internalKmer.write(out);
        if ((checkMessage & CheckMessage.NEIGHBER) != 0)
            neighberNode.write(out);
        if ((checkMessage & CheckMessage.NODEIDLIST) != 0)
            nodeIdList.write(out);
        if ((checkMessage & CheckMessage.START) != 0)
            startVertexId.write(out);
        if ((checkMessage & CheckMessage.PATHLIST) != 0)
            pathList.write(out);
        out.writeFloat(averageCoverage);
        out.writeBoolean(isFlip);
        out.writeByte(flag); 
        out.writeBoolean(updateMsg);
        out.writeBoolean(srcFlip);
        out.writeBoolean(destFlip);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        kmerlength = in.readInt();
        this.reset(kmerlength);
        checkMessage = in.readByte();
        if ((checkMessage & CheckMessage.SOURCE) != 0)
            sourceVertexId.readFields(in);
        if ((checkMessage & CheckMessage.INTERNALKMER) != 0)
            internalKmer.readFields(in);
        if ((checkMessage & CheckMessage.NEIGHBER) != 0)
            neighberNode.readFields(in);
        if ((checkMessage & CheckMessage.NODEIDLIST) != 0)
            nodeIdList.readFields(in);
        if ((checkMessage & CheckMessage.START) != 0)
            startVertexId.readFields(in);
        if ((checkMessage & CheckMessage.PATHLIST) != 0)
            pathList.readFields(in);
        averageCoverage = in.readFloat();
        isFlip = in.readBoolean();
        flag = in.readByte();
        updateMsg = in.readBoolean();
        srcFlip = in.readBoolean();
        destFlip = in.readBoolean();
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
    
    public static final class SortByCoverage implements Comparator<MessageWritable> {
        @Override
        public int compare(MessageWritable left, MessageWritable right) {
            return Float.compare(left.averageCoverage, right.averageCoverage);
        }
    }
    
    /**
     * Update my coverage to be the average of this and other. Used when merging paths.
     */
    public void mergeCoverage(MessageWritable other) {
        // sequence considered in the average doesn't include anything overlapping with other kmers
        float adjustedLength = internalKmer.getKmerLetterLength() + other.internalKmer.getKmerLetterLength() - (KmerBytesWritable.getKmerLength() - 1) * 2;
        
        float myCount = (internalKmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() - 1) * averageCoverage;
        float otherCount = (other.internalKmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() - 1) * other.averageCoverage;
        averageCoverage = (myCount + otherCount) / adjustedLength;
    }

    @Override
    public int sizeInBytes() {
        // TODO Auto-generated method stub
        return 0;
    }
}
