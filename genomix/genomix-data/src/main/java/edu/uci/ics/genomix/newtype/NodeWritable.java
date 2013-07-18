package edu.uci.ics.genomix.newtype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.newtype.PositionListWritable;

public class NodeWritable implements WritableComparable<NodeWritable>, Serializable{

    private static final long serialVersionUID = 1L;
    public static final NodeWritable EMPTY_NODE = new NodeWritable(0);
    
    private PositionListWritable forwardForwardList;
    private PositionListWritable forwardReverseList;
    private PositionListWritable reverseForwardList;
    private PositionListWritable reverseReverseList;
    private KmerBytesWritable kmer;
    
    // merge/update directions
    public static class DirectionFlag {
        public static final byte DIR_FF = 0b00 << 0;
        public static final byte DIR_FR = 0b01 << 0;
        public static final byte DIR_RF = 0b10 << 0;
        public static final byte DIR_RR = 0b11 << 0;
        public static final byte DIR_MASK = 0b11 << 0;
    }
    
    public NodeWritable() {
        this(21);
    }
    
    public NodeWritable(int kmerSize) {
        forwardForwardList = new PositionListWritable();
        forwardReverseList = new PositionListWritable();
        reverseForwardList = new PositionListWritable();
        reverseReverseList = new PositionListWritable();
        kmer = new KmerBytesWritable(kmerSize);
    }
    
    public NodeWritable(PositionListWritable FFList, PositionListWritable FRList,
            PositionListWritable RFList, PositionListWritable RRList, KmerBytesWritable kmer) {
        this(kmer.getKmerLength());
        set(FFList, FRList, RFList, RRList, kmer);
    }
    
    public void set(NodeWritable node){
        set(node.forwardForwardList, node.forwardReverseList, node.reverseForwardList, 
                node.reverseReverseList, node.kmer);
    }
    
    public void set(PositionListWritable FFList, PositionListWritable FRList,
            PositionListWritable RFList, PositionListWritable RRList, KmerBytesWritable kmer) {
        this.forwardForwardList.set(FFList);
        this.forwardReverseList.set(FRList);
        this.reverseForwardList.set(RFList);
        this.reverseReverseList.set(RRList);
        this.kmer.set(kmer);
    }

    public void reset(int kmerSize) {
        forwardForwardList.reset();
        forwardReverseList.reset();
        reverseForwardList.reset();
        reverseReverseList.reset();
        kmer.reset(kmerSize);
    }
    
    public KmerBytesWritable getKmer() {
        return kmer;
    }

    public void setKmer(KmerBytesWritable kmer) {
        this.kmer = kmer;
    }
    
    public int getCount() {
        return kmer.getKmerLength();
    }
    
    public PositionListWritable getFFList() {
        return forwardForwardList;
    }

    public PositionListWritable getFRList() {
        return forwardReverseList;
    }

    public PositionListWritable getRFList() {
        return reverseForwardList;
    }

    public PositionListWritable getRRList() {
        return reverseReverseList;
    }
    
    public PositionListWritable getListFromDir(byte dir) {
        switch (dir & DirectionFlag.DIR_MASK) {
            case DirectionFlag.DIR_FF:
                return getFFList();
            case DirectionFlag.DIR_FR:
                return getFRList();
            case DirectionFlag.DIR_RF:
                return getRFList();
            case DirectionFlag.DIR_RR:
                return getRRList();
            default:
                throw new RuntimeException("Unrecognized direction in getListFromDir: " + dir);
        }
    }
    @Override
    public void write(DataOutput out) throws IOException {
        this.forwardForwardList.write(out);
        this.forwardReverseList.write(out);
        this.reverseForwardList.write(out);
        this.reverseReverseList.write(out);
        this.kmer.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.forwardForwardList.readFields(in);
        this.forwardReverseList.readFields(in);
        this.reverseForwardList.readFields(in);
        this.reverseReverseList.readFields(in);
        this.kmer.readFields(in);
    }

    @Override
    public int compareTo(NodeWritable other) {
        return this.kmer.compareTo(other.kmer);
    }
    
    @Override
    public int hashCode() {
        return this.kmer.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof NodeWritable) {
            NodeWritable nw = (NodeWritable) o;
            return (this.forwardForwardList.equals(nw.forwardForwardList)
                    && this.forwardReverseList.equals(nw.forwardReverseList)
                    && this.reverseForwardList.equals(nw.reverseForwardList)
                    && this.reverseReverseList.equals(nw.reverseReverseList) && this.kmer.equals(nw.kmer));
        }
        return false;
    }
    
    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('(');
        sbuilder.append(forwardForwardList.toString()).append('\t');
        sbuilder.append(forwardReverseList.toString()).append('\t');
        sbuilder.append(reverseForwardList.toString()).append('\t');
        sbuilder.append(reverseReverseList.toString()).append('\t');
        sbuilder.append(kmer.toString()).append(')');
        return sbuilder.toString();
    }

    public void mergeForwardNext(NodeWritable nextNode, int initialKmerSize) {
        this.forwardForwardList.set(nextNode.forwardForwardList);
        this.forwardReverseList.set(nextNode.forwardReverseList);
        kmer.mergeWithFFKmer(initialKmerSize, nextNode.getKmer());
    }

    public void mergeForwardPre(NodeWritable preNode, int initialKmerSize) {
        this.reverseForwardList.set(preNode.reverseForwardList);
        this.reverseReverseList.set(preNode.reverseReverseList);
        kmer.mergeWithRRKmer(initialKmerSize, preNode.getKmer());
    }
    
    public int inDegree() {
        return reverseReverseList.getCountOfPosition() + reverseForwardList.getCountOfPosition();
    }

    public int outDegree() {
        return forwardForwardList.getCountOfPosition() + forwardReverseList.getCountOfPosition();
    }

    /*
     * Return if this node is a "path" compressible node, that is, it has an in-degree and out-degree of 1 
     */
    public boolean isPathNode() {
        return inDegree() == 1 && outDegree() == 1;
    }

    public boolean isSimpleOrTerminalPath() {
        return isPathNode() || (inDegree() == 0 && outDegree() == 1) || (inDegree() == 1 && outDegree() == 0);
    }
}
