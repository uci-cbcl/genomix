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

public class NodeWritable implements WritableComparable<NodeWritable>, Serializable {

    private static final long serialVersionUID = 1L;
    public static final NodeWritable EMPTY_NODE = new NodeWritable();

    private static final int SIZE_FLOAT = 4;
    
    // edge list
    private VKmerListWritable edges[] = {null, null, null, null};
    
    // connections within the same read -- used for resolving repeats and scaffolding
    private PositionListWritable threads[] = {null, null, null, null};
    
    private PositionListWritable startReads;  // first internalKmer in read (or last but internalKmer was flipped)
    private PositionListWritable endReads;  //last internalKmer in read (or first but internalKmer was flipped)
    
    private VKmerBytesWritable internalKmer;

    private float averageCoverage;

    // merge/update directions

    // merge/update directions
    
    // merge/update directions
    public static class DirectionFlag {
        public static final byte DIR_FF = 0b00 << 0;
        public static final byte DIR_FR = 0b01 << 0;
        public static final byte DIR_RF = 0b10 << 0;
        public static final byte DIR_RR = 0b11 << 0;
        public static final byte DIR_MASK = 0b11 << 0;
        public static final byte DIR_CLEAR = 0b1111100 << 0;
        
        public static final byte[] values = {DIR_FF, DIR_FR, DIR_RF, DIR_RR};
    }
        
    public NodeWritable() {
        for (byte d: DirectionFlag.values) {
            edges[d] = new VKmerListWritable();
            threads[d] = new PositionListWritable();
        }
        startReads = new PositionListWritable();
        endReads = new PositionListWritable();
        internalKmer = new VKmerBytesWritable();  // in graph construction - not set kmerlength Optimization: VKmer
        averageCoverage = 0;
    }
    
    public NodeWritable(VKmerListWritable[] edges, PositionListWritable[] threads,
            PositionListWritable startReads, PositionListWritable endReads,
            VKmerBytesWritable kmer, float coverage) {
        this();
        set(edges, threads, startReads, endReads, kmer, coverage);
    }
    
    public void set(NodeWritable node){
        set(node.edges, node.threads, node.startReads, node.endReads, node.internalKmer, node.averageCoverage);
    }
    
    public void set(VKmerListWritable[] edges, PositionListWritable[] threads,
            PositionListWritable startReads, PositionListWritable endReads, 
            VKmerBytesWritable kmer2, float coverage) {
        for (byte d: DirectionFlag.values) {
            this.edges[d].setCopy(edges[d]);
            this.threads[d].set(threads[d]);
        }
        this.startReads.set(startReads);
        this.endReads.set(endReads);
        this.internalKmer.setAsCopy(kmer2);
        this.averageCoverage = coverage;
    }

    public void reset() {
        for (byte d: DirectionFlag.values) {
            edges[d].reset();
            threads[d].reset();
        }
        startReads.reset();
        endReads.reset();
        internalKmer.reset(0);
        averageCoverage = 0;
    }
    
    public VKmerBytesWritable getInternalKmer() {
        return internalKmer;
    }

    public void setInternalKmer(VKmerBytesWritable internalKmer) {
        this.internalKmer.setAsCopy(internalKmer);
    }

    public int getKmerLength() {
        return internalKmer.getKmerLetterLength();
    }
    
    public VKmerListWritable getEdgeList(byte dir) {
        return edges[dir & DirectionFlag.DIR_MASK];
    }
    
    public void setEdgeList(byte dir, VKmerListWritable edgeList) {
        this.edges[dir & DirectionFlag.DIR_MASK].setCopy(edgeList);
    }
    
    public PositionListWritable getThreadList(byte dir) {
        return threads[dir & DirectionFlag.DIR_MASK];
    }
    
    public void setThreadList(byte dir, PositionListWritable threadList) {
        this.threads[dir & DirectionFlag.DIR_MASK].set(threadList);
    }
	
	/**
	 * Update my coverage to be the average of this and other. Used when merging paths.
	 */
	public void mergeCoverage(NodeWritable other) {
	    // sequence considered in the average doesn't include anything overlapping with other kmers
	    float adjustedLength = internalKmer.getKmerLetterLength() + other.internalKmer.getKmerLetterLength() - (KmerBytesWritable.getKmerLength() - 1) * 2;
	    
	    float myCount = (internalKmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() - 1) * averageCoverage;
	    float otherCount = (other.internalKmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() - 1) * other.averageCoverage;
	    averageCoverage = (myCount + otherCount) / adjustedLength;
	}
	
	/**
	 * Update my coverage as if all the reads in other became my own 
	 */
	public void addCoverage(NodeWritable other) {
	    float myAdjustedLength = internalKmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() - 1;
	    float otherAdjustedLength = other.internalKmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() - 1; 
	    averageCoverage += other.averageCoverage * (otherAdjustedLength / myAdjustedLength);
	}
	
	public void setAvgCoverage(float coverage) {
	    averageCoverage = coverage;
	}
	
	public float getAvgCoverage() {
	    return averageCoverage;
	}
	
	public PositionListWritable getStartReads() {
        return startReads;
    }

    public void setStartReads(PositionListWritable startReads) {
        this.startReads.set(startReads);
    }

    public PositionListWritable getEndReads() {
        return endReads;
    }

    public void setEndReads(PositionListWritable endReads) {
        this.endReads.set(endReads);
    }

    /**
	 * Returns the length of the byte-array version of this node
	 */
	public int getSerializedLength() {
	    int length = 0;
	    for (byte d:DirectionFlag.values) {
	        length += edges[d].getLength();
	        length += threads[d].getLength();
	    }
	    length += internalKmer.getLength();
	    length += this.startReads.getLength();
	    length += this.endReads.getLength();
	    length += SIZE_FLOAT;
	    return length;
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
        for (byte d:DirectionFlag.values) {
            edges[d].setCopy(data, curOffset);
            curOffset += edges[d].getLength();
        }
        for (byte d:DirectionFlag.values) {
            threads[d].set(data, curOffset);
            curOffset += threads[d].getLength();
        }
        startReads.set(data, curOffset);
        curOffset += startReads.getLength();
        endReads.set(data, curOffset);
        curOffset += endReads.getLength();
        internalKmer.setAsCopy(data, curOffset);
        curOffset += internalKmer.getLength();
        averageCoverage = Marshal.getFloat(data, curOffset);
    }

    public void setAsReference(byte[] data, int offset) {
        int curOffset = offset;
        for (byte d:DirectionFlag.values) {
            edges[d].setNewReference(data, curOffset);
            curOffset += edges[d].getLength();
        }
        for (byte d:DirectionFlag.values) {
            threads[d].setNewReference(data, curOffset);
            curOffset += threads[d].getLength();
        }
        startReads.setNewReference(data, curOffset);
        curOffset += startReads.getLength();
        endReads.setNewReference(data, curOffset);
        curOffset += endReads.getLength();
        
        internalKmer.setAsReference(data, curOffset);        
        curOffset += internalKmer.getLength();
        averageCoverage = Marshal.getFloat(data, curOffset);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (byte d:DirectionFlag.values) {
            edges[d].write(out);
        }
        for (byte d:DirectionFlag.values) {
            threads[d].write(out);
        }
        startReads.write(out);
        endReads.write(out);
        this.internalKmer.write(out);
        out.writeFloat(averageCoverage);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        for (byte d:DirectionFlag.values) {
            edges[d].readFields(in);
        }
        for (byte d:DirectionFlag.values) {
            threads[d].readFields(in);
        }
        startReads.readFields(in);
        endReads.readFields(in);
        this.internalKmer.readFields(in);
        averageCoverage = in.readFloat();
    }

    @Override
    public int compareTo(NodeWritable other) {
        return this.internalKmer.compareTo(other.internalKmer);
    }

    public class SortByCoverage implements Comparator<NodeWritable> {
        @Override
        public int compare(NodeWritable left, NodeWritable right) {
            return Float.compare(left.averageCoverage, right.averageCoverage);
        }
    }

    @Override
    public int hashCode() {
        return this.internalKmer.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof NodeWritable))
            return false;
            
        NodeWritable nw = (NodeWritable) o;
        for (byte d:DirectionFlag.values) {
            if (!edges[d].equals(nw.edges[d]) || !threads[d].equals(nw.threads[d]))
                return false;
        }
        return averageCoverage == nw.averageCoverage && internalKmer.equals(nw.internalKmer);
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        for (byte d: DirectionFlag.values) {
            sbuilder.append(edges[d].toString()).append('\t');
        }
        for (byte d: DirectionFlag.values) {
            sbuilder.append(threads[d].toString()).append('\t');
        }
        sbuilder.append(internalKmer.toString()).append('\t');
        sbuilder.append(averageCoverage).append('x').append('}');
        return sbuilder.toString();
    }

    public void mergeForwardNext(final NodeWritable nextNode, int initialKmerSize) {
        edges[DirectionFlag.DIR_FF].setCopy(nextNode.edges[DirectionFlag.DIR_FF]);
        edges[DirectionFlag.DIR_FR].setCopy(nextNode.edges[DirectionFlag.DIR_FR]);
        internalKmer.mergeWithFFKmer(initialKmerSize, nextNode.getInternalKmer());
    }

    public void mergeForwardPre(final NodeWritable preNode, int initialKmerSize) {
        edges[DirectionFlag.DIR_RF].setCopy(preNode.edges[DirectionFlag.DIR_RF]);
        edges[DirectionFlag.DIR_RR].setCopy(preNode.edges[DirectionFlag.DIR_RR]);
        internalKmer.mergeWithRRKmer(initialKmerSize, preNode.getInternalKmer());
    }

    public int inDegree() {
        return edges[DirectionFlag.DIR_RR].getCountOfPosition() + edges[DirectionFlag.DIR_RF].getCountOfPosition();
    }

    public int outDegree() {
        return edges[DirectionFlag.DIR_FF].getCountOfPosition() + edges[DirectionFlag.DIR_FR].getCountOfPosition();
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
