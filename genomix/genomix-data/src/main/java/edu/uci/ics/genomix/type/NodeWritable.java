package edu.uci.ics.genomix.type;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.data.Marshal;


public class NodeWritable implements WritableComparable<NodeWritable>, Serializable{

    private static final long serialVersionUID = 1L;
    public static final NodeWritable EMPTY_NODE = new NodeWritable();
    
    private static final int SIZE_FLOAT = 4;
    
    // edge list
    private VKmerListWritable edges[] = {null, null, null, null};
    
    // connections within the same read -- used for resolving repeats and scaffolding
    private PositionListWritable threads[] = {null, null, null, null};
    
    private PositionListWritable startReads;  // first kmer in read (or last but kmer was flipped)
    private PositionListWritable endReads;  //last kmer in read (or first but kmer was flipped)
    
    private VKmerBytesWritable kmer;
    private float averageCoverage;
    
    // merge/update directions
    public static class DirectionFlag {
        public static final byte DIR_FF = 0b00 << 0;
        public static final byte DIR_FR = 0b01 << 0;
        public static final byte DIR_RF = 0b10 << 0;
        public static final byte DIR_RR = 0b11 << 0;
        public static final byte DIR_MASK = 0b11 << 0;
    }
    
    public enum Dir {
        FF(0b00 << 0),
        FR(0b01 << 0),
        RF(0b10 << 0),
        RR(0b11 << 0);
        
		private byte value;
        private Dir(int val) {
            value = (byte) val;
        }
        public byte getValue() {
        	return value;
        }
    }
    
    public NodeWritable() {
        for (Dir d: Dir.values()) {
            edges[d.getValue()] = new VKmerListWritable();
            threads[d.getValue()] = new PositionListWritable();
        }
        startReads = new PositionListWritable();
        endReads = new PositionListWritable();
        kmer = new VKmerBytesWritable();  // in graph construction - not set kmerlength Optimization: VKmer
        averageCoverage = 0;
    }
    
    public NodeWritable(VKmerListWritable[] edges, PositionListWritable[] threads,
            PositionListWritable startReads, PositionListWritable endReads,
            VKmerBytesWritable kmer, float coverage) {
        this();
        set(edges, threads, startReads, endReads, kmer, coverage);
    }
    
    public void set(NodeWritable node){
        set(node.edges, node.threads, node.startReads, node.endReads, node.kmer, node.averageCoverage);
    }
    
    public void set(VKmerListWritable[] edges, PositionListWritable[] threads,
            PositionListWritable startReads, PositionListWritable endReads, 
            VKmerBytesWritable kmer2, float coverage) {
        for (Dir d: Dir.values()) {
            this.edges[d.getValue()].setCopy(edges[d.getValue()]);
            this.threads[d.getValue()].set(threads[d.getValue()]);
        }
        this.kmer.setAsCopy(kmer2);
        this.averageCoverage = coverage;
    }

    public void reset() {
        for (Dir d: Dir.values()) {
            edges[d.getValue()].reset();
            threads[d.getValue()].reset();
        }
        this.kmer.reset(0);
        averageCoverage = 0;
    }
    
    public PositionListWritable getNodeIdList() {
        return nodeIdList;
    }

    public void setNodeIdList(PositionListWritable nodeIdList) {
        this.nodeIdList.set(nodeIdList);
    }

    public VKmerBytesWritable getKmer() {
        return kmer;
    }

    public void setKmer(VKmerBytesWritable kmer) {
        this.kmer.setAsCopy(kmer);
    }
    
    public int getKmerLength() {
        return kmer.getKmerLetterLength();
    }
    
    public VKmerListWritable getFFList() {
        return forwardForwardList;
    }

    public VKmerListWritable getFRList() {
        return forwardReverseList;
    }

    public VKmerListWritable getRFList() {
        return reverseForwardList;
    }

    public VKmerListWritable getRRList() {
        return reverseReverseList;
    }
    
	public void setFFList(VKmerListWritable forwardForwardList) {
		this.forwardForwardList.setCopy(forwardForwardList);
	}

	public void setFRList(VKmerListWritable forwardReverseList) {
		this.forwardReverseList.setCopy(forwardReverseList);
	}

	public void setRFList(VKmerListWritable reverseForwardList) {
		this.reverseForwardList.setCopy(reverseForwardList);
	}

	public void setRRList(VKmerListWritable reverseReverseList) {
		this.reverseReverseList.setCopy(reverseReverseList);
	}

	public VKmerListWritable getListFromDir(byte dir) {
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
	
	/**
	 * Update my coverage to be the average of this and other. Used when merging paths.
	 */
	public void mergeCoverage(NodeWritable other) {
	    // sequence considered in the average doesn't include anything overlapping with other kmers
	    float adjustedLength = kmer.getKmerLetterLength() + other.kmer.getKmerLetterLength() - (KmerBytesWritable.getKmerLength() - 1) * 2;
	    
	    float myCount = (kmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() - 1) * averageCoverage;
	    float otherCount = (other.kmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() - 1) * other.averageCoverage;
	    averageCoverage = (myCount + otherCount) / adjustedLength;
	}
	
	/**
	 * Update my coverage as if all the reads in other became my own 
	 */
	public void addCoverage(NodeWritable other) {
	    float myAdjustedLength = kmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() - 1;
	    float otherAdjustedLength = other.kmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() - 1; 
	    averageCoverage += other.averageCoverage * (otherAdjustedLength / myAdjustedLength);
	}
	
	public void setAvgCoverage(float coverage) {
	    averageCoverage = coverage;
	}
	
	public float getAvgCoverage() {
	    return averageCoverage;
	}
	
	/**
	 * Returns the length of the byte-array version of this node
	 */
	public int getSerializedLength() {
	    return nodeIdList.getLength() + forwardForwardList.getLength() + forwardReverseList.getLength() + 
	            reverseForwardList.getLength() + reverseReverseList.getLength() + kmer.getLength() + SIZE_FLOAT;
	}
	
	/**
     * Return this Node's representation as a new byte array 
     */
    public byte[] marshalToByteArray() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(getSerializedLength());
        DataOutputStream out = new DataOutputStream(baos);
        write(out);
        return baos.toByteArray();
    }
    
    public void setAsCopy(byte[] data, int offset) {
        int curOffset = offset;
        nodeIdList.set(data, curOffset);
        
        curOffset += nodeIdList.getLength();
        forwardForwardList.setCopy(data, curOffset);
        curOffset += forwardForwardList.getLength();
        forwardReverseList.setCopy(data, curOffset);
        curOffset += forwardReverseList.getLength();
        reverseForwardList.setCopy(data, curOffset);
        curOffset += reverseForwardList.getLength();
        reverseReverseList.setCopy(data, curOffset);
        
        curOffset += reverseReverseList.getLength();
        kmer.setAsCopy(data, curOffset);
        
        curOffset += kmer.getLength();
        averageCoverage = Marshal.getFloat(data, curOffset);
    }
    
    public void setAsReference(byte[] data, int offset) {
        int curOffset = offset;
        nodeIdList.setNewReference(data, curOffset);
        
        curOffset += nodeIdList.getLength();
        forwardForwardList.setNewReference(data, curOffset);
        curOffset += forwardForwardList.getLength();
        forwardReverseList.setNewReference(data, curOffset);
        curOffset += forwardReverseList.getLength();
        reverseForwardList.setNewReference(data, curOffset);
        curOffset += reverseForwardList.getLength();
        reverseReverseList.setNewReference(data, curOffset);
        
        curOffset += reverseReverseList.getLength();
        kmer.setAsReference(data, curOffset);
        
        curOffset += kmer.getLength();
        averageCoverage = Marshal.getFloat(data, curOffset);
    }
	
    @Override
    public void write(DataOutput out) throws IOException {
        this.nodeIdList.write(out);
        this.forwardForwardList.write(out);
        this.forwardReverseList.write(out);
        this.reverseForwardList.write(out);
        this.reverseReverseList.write(out);
        this.kmer.write(out);
        out.writeFloat(averageCoverage);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        this.nodeIdList.readFields(in);
        this.forwardForwardList.readFields(in);
        this.forwardReverseList.readFields(in);
        this.reverseForwardList.readFields(in);
        this.reverseReverseList.readFields(in);
        this.kmer.readFields(in);
        averageCoverage = in.readFloat();
    }

    @Override
    public int compareTo(NodeWritable other) {
        return this.kmer.compareTo(other.kmer);
    }
    
    public class SortByCoverage implements Comparator<NodeWritable> {
        @Override
        public int compare(NodeWritable left, NodeWritable right) {
            return Float.compare(left.averageCoverage, right.averageCoverage);
        }
    }
    
    @Override
    public int hashCode() {
        return this.kmer.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof NodeWritable) {
            NodeWritable nw = (NodeWritable) o;
            return (this.nodeIdList.equals(nw.nodeIdList)
                    && this.forwardForwardList.equals(nw.forwardForwardList)
                    && this.forwardReverseList.equals(nw.forwardReverseList)
                    && this.reverseForwardList.equals(nw.reverseForwardList)
                    && this.reverseReverseList.equals(nw.reverseReverseList) && this.kmer.equals(nw.kmer));
        }
        return false;
    }
    
    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        sbuilder.append(nodeIdList.toString()).append('\t');
        sbuilder.append(forwardForwardList.toString()).append('\t');
        sbuilder.append(forwardReverseList.toString()).append('\t');
        sbuilder.append(reverseForwardList.toString()).append('\t');
        sbuilder.append(reverseReverseList.toString()).append('\t');
        sbuilder.append(kmer.toString()).append('\t');
        sbuilder.append(averageCoverage).append('x').append('}');
        return sbuilder.toString();
    }

    public void mergeForwardNext(final NodeWritable nextNode, int initialKmerSize) {
        this.forwardForwardList.setCopy(nextNode.forwardForwardList);
        this.forwardReverseList.setCopy(nextNode.forwardReverseList);
        kmer.mergeWithFFKmer(initialKmerSize, nextNode.getKmer());
    }

    public void mergeForwardPre(final NodeWritable preNode, int initialKmerSize) {
        this.reverseForwardList.setCopy(preNode.reverseForwardList);
        this.reverseReverseList.setCopy(preNode.reverseReverseList);
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
