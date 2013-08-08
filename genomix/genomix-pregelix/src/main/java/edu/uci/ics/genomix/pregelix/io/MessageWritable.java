package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.type.CheckMessage;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.pregelix.api.io.WritableSizable;

public class MessageWritable implements WritableComparable<MessageWritable>, WritableSizable {
    /**
     * sourceVertexId stores source vertexId when headVertex sends the message
     * stores neighber vertexValue when pathVertex sends the message
     * file stores the point to the file that stores the chains of connected DNA
     */
    private VKmerBytesWritable sourceVertexId;
    private VKmerBytesWritable actualKmer;
    private AdjacencyListWritable neighberNode; //incoming or outgoing
    private PositionListWritable nodeIdList = new PositionListWritable();
    private float averageCoverage;
    private byte flag;
    private boolean isFlip; // also use for odd or even
    private int kmerlength = 0;
    private boolean updateMsg = false;
    private VKmerBytesWritable startVertexId;
    private VKmerListWritable pathList;
    
    private byte checkMessage;

    public MessageWritable() {
        sourceVertexId = new VKmerBytesWritable();
        actualKmer = new VKmerBytesWritable();
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
        actualKmer = new VKmerBytesWritable(0);

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
        if (actualKmer != null) {
            checkMessage |= CheckMessage.ACUTUALKMER;
            this.actualKmer.setAsCopy(msg.getActualKmer());

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
            checkMessage |= CheckMessage.ACUTUALKMER;
            this.actualKmer.setAsCopy(chainVertexId);

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
//        actualKmer.reset();
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
    
    public VKmerBytesWritable getActualKmer() {
        return actualKmer;
    }

    public void setActualKmer(VKmerBytesWritable actualKmer) {
        if (actualKmer != null) {
            checkMessage |= CheckMessage.ACUTUALKMER;
            this.actualKmer.setAsCopy(actualKmer);
        }
    }
    
    // use for scaffolding
    public VKmerBytesWritable getMiddleVertexId() {
        return actualKmer;
    }

    public void setMiddleVertexId(VKmerBytesWritable middleKmer) {
        if (middleKmer != null) {
            checkMessage |= CheckMessage.ACUTUALKMER;
            this.actualKmer.setAsCopy(middleKmer);
        }
    }
    
    public VKmerBytesWritable getCreatedVertexId() {
        return actualKmer;
    }

    public void setCreatedVertexId(VKmerBytesWritable actualKmer) {
        if (actualKmer != null) {
            checkMessage |= CheckMessage.ACUTUALKMER;
            this.actualKmer.setAsCopy(actualKmer);
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
        return actualKmer.getKmerLetterLength();
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

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(kmerlength);
        out.writeByte(checkMessage);
        if ((checkMessage & CheckMessage.SOURCE) != 0)
            sourceVertexId.write(out);
        if ((checkMessage & CheckMessage.ACUTUALKMER) != 0)
            actualKmer.write(out);
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
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        kmerlength = in.readInt();
        this.reset(kmerlength);
        checkMessage = in.readByte();
        if ((checkMessage & CheckMessage.SOURCE) != 0)
            sourceVertexId.readFields(in);
        if ((checkMessage & CheckMessage.ACUTUALKMER) != 0)
            actualKmer.readFields(in);
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
        float adjustedLength = actualKmer.getKmerLetterLength() + other.actualKmer.getKmerLetterLength() - (KmerBytesWritable.getKmerLength() - 1) * 2;
        
        float myCount = (actualKmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() - 1) * averageCoverage;
        float otherCount = (other.actualKmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() - 1) * other.averageCoverage;
        averageCoverage = (myCount + otherCount) / adjustedLength;
    }

    @Override
    public int sizeInBytes() {
        // TODO Auto-generated method stub
        return 0;
    }
}
