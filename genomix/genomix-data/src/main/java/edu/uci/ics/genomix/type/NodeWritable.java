/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.genomix.type;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.Marshal;

public class NodeWritable implements Writable, Serializable {
	
	public enum DIR {
		
	    REVERSE((byte) (0b01 << 2)),
		FORWARD((byte) (0b10 << 2));
		
		public static final byte MASK = (byte)(0b11 << 2); 
		
		private final byte val;
		private DIR(byte val) {
	        this.val = val;
	    }
		public final byte get() {
		    return val;
		}
		public static DIR mirror(DIR direction) {
		    switch (direction) {
		        case REVERSE:
		            return FORWARD;
		        case FORWARD:
		            return REVERSE;
		        default:
		            throw new IllegalArgumentException("Invalid direction given: " + direction);
		    }
		}
		public DIR mirror() {
		    return mirror(this);
		}
		
		public static byte fromSet(EnumSet<DIR> set) {
            byte b = 0;
            if (set.contains(REVERSE))
                b |= REVERSE.val;
            if (set.contains(FORWARD))
                b |= FORWARD.val;
            return b;  
        }
		public final EnumSet<EDGETYPE> edgeType(){
		    return edgeTypesInDir(this);
		}
		
	    public static final EnumSet<EDGETYPE> edgeTypesInDir(DIR direction) {
	        return direction == DIR.REVERSE ? EDGETYPE.INCOMING : EDGETYPE.OUTGOING;
	    }
	    
		public static EnumSet<DIR> enumSetFromByte(short s) {  //TODO change shorts to byte? (anbangx) 
		    EnumSet<DIR> retSet = EnumSet.noneOf(DIR.class);
		    if ((s & REVERSE.get()) != 0)
		        retSet.add(DIR.REVERSE);
		    if ((s & FORWARD.get()) != 0)
                retSet.add(DIR.FORWARD);
		    return retSet;
		}
		
		/**
		 * Given a byte representing NEXT, PREVIOUS, or both, return an enumset representing PREVIOUS, NEXT, or both, respectively. 
		 */
		public static EnumSet<DIR> flipSetFromByte(short s) {
            EnumSet<DIR> retSet = EnumSet.noneOf(DIR.class);
            if ((s & REVERSE.get()) != 0)
                retSet.add(DIR.FORWARD);
            if ((s & FORWARD.get()) != 0)
                retSet.add(DIR.REVERSE);
            return retSet;
        }
	}
	
    public enum EDGETYPE {
            
            FF((byte)(0b00 << 0)),
            FR((byte)(0b01 << 0)),
            RF((byte)(0b10 << 0)),
            RR((byte)(0b11 << 0));
            
            public static final byte MASK = (byte)(0b11 << 0); 
            public static final byte CLEAR = (byte)(0b1111100 << 0);
            private final byte val;
            
            private EDGETYPE(byte val){
                this.val = val;
            }
            
            public final byte get(){
                return val;
            }
            
            public static final EnumSet<EDGETYPE> INCOMING = EnumSet.of(RF, RR);
            public static final EnumSet<EDGETYPE> OUTGOING = EnumSet.of(FF, FR);
                    
            public static EDGETYPE fromByte(short b) {
                b &= MASK;
                if(b == FF.val)
                    return FF;
                if(b == FR.val)
                    return FR;
                if(b == RF.val)
                    return RF;
                if(b == RR.val)
                    return RR;
                return null;
    
            }
            /**
             * Returns the edge dir for B->A when the A->B edge is type @dir
             */
            public EDGETYPE mirror(){
                return mirror(this);
            }
            
            public static EDGETYPE mirror(EDGETYPE edgeType){
                switch (edgeType) {
                    case FF:
                        return RR;
                    case FR:
                        return FR;
                    case RF:
                        return RF;
                    case RR:
                        return FF;
                    default:
                        throw new RuntimeException("Unrecognized direction in mirrorDirection: " + edgeType);
                }
            }
            
            public DIR dir() {
                return dir(this);
            }
            
            public static DIR dir(EDGETYPE edgeType){ // .dir static / non-static
                switch(edgeType){
                    case FF:
                    case FR:
                        return DIR.FORWARD;
                    case RF:
                    case RR:
                        return DIR.REVERSE;
                    default:
                        throw new RuntimeException("Unrecognized direction in dirFromEdgeType: " + edgeType);
                }
            }
            
            /**
             * return the edgetype corresponding to moving across edge1 and edge2.
             * 
             *  So if A <-e1- B -e2-> C, we will return the relationship from A -> C
             *  
             *  If the relationship isn't a valid path (e.g., e1,e2 are both FF), an exception is raised.
             */
            public static EDGETYPE resolveEdgeThroughPath(EDGETYPE BtoA, EDGETYPE BtoC) {
                EDGETYPE AtoB = mirror(BtoA);
                // a valid path must exist from A to C
                // specifically, two rules apply for AtoB and BtoC
                //      1) the internal letters must be the same (so FF, RF will be an error)
                //      2) the final direction is the 1st letter of AtoB + 2nd letter of BtoC
                // TODO? maybe we could use the string version to resolve this following above rules
                switch(AtoB) {
                    case FF:
                        switch (BtoC) {
                            case FF:
                            case FR:
                                return BtoC;
                            case RF:
                            case RR:
                                throw new IllegalArgumentException("Tried to resolve an invalid link type: A --" + AtoB + "--> B --" + BtoC + "--> C");
                        }
                        break;
                    case FR:
                        switch (BtoC) {
                            case FF:
                            case FR:
                                throw new IllegalArgumentException("Tried to resolve an invalid link type: A --" + AtoB + "--> B --" + BtoC + "--> C");
                            case RF:
                                return FF;
                            case RR:
                                return FR;
                        }
                        break;
                    case RF:
                        switch (BtoC) {
                            case FF:
                                return RF;
                            case FR:
                                return RR;
                            case RF:
                            case RR:
                                throw new IllegalArgumentException("Tried to resolve an invalid link type: A --" + AtoB + "--> B --" + BtoC + "--> C");
                        }
                        break;
                    case RR:
                        switch (BtoC) {
                            case FF:
                            case FR:
                                throw new IllegalArgumentException("Tried to resolve an invalid link type: A --" + AtoB + "--> B --" + BtoC + "--> C");
                            case RF:
                                return RF;
                            case RR:
                                return RR;
                        }
                        break;
                }
                throw new IllegalStateException("Logic Error or unrecognized direction... original values were: " + BtoA + " and " + BtoC);
            }
            
            public boolean causesFlip(){
                return causesFlip(this);
            }
            public static boolean causesFlip(EDGETYPE edgeType) {
                switch (edgeType) {
                    case FF:
                    case RR:
                        return false;
                    case FR:
                    case RF:
                        return true;
                    default:
                        throw new IllegalArgumentException("unrecognized direction: " + edgeType);
                }
            }
            
            public EDGETYPE flipNeighbor(){
                return flipNeighbor(this);
            }
            
            public static EDGETYPE flipNeighbor(EDGETYPE neighborToMe){
                switch (neighborToMe) {
                    case FF:
                        return FR;
                    case FR:
                        return FF;
                    case RF:
                        return RR;
                    case RR:
                        return RF;
                    default:
                        throw new RuntimeException("Unrecognized direction for neighborDir: " + neighborToMe);
                }
            }
            
            public static boolean sameOrientation(EDGETYPE et1, EDGETYPE et2){
                return et1.causesFlip() != et2.causesFlip();
            }
            
            public static boolean sameOrientation(byte b1, byte b2){
                EDGETYPE et1 = EDGETYPE.fromByte(b1);
                EDGETYPE et2 = EDGETYPE.fromByte(b2);
                return sameOrientation(et1, et2);
            }
        }
    
    public static class NeighborInfo {
        public EDGETYPE et;
        public EdgeWritable edge;
        public VKmerBytesWritable kmer;
        
        public NeighborInfo(EDGETYPE edgeType, EdgeWritable edgeWritable) {
            et = edgeType;
            edge = edgeWritable;
            kmer = edge.getKey();
        }
    }
    
    public static class NeighborsInfo implements Iterable<NeighborInfo>{
        public final EDGETYPE et;
        public final EdgeListWritable edges;
        
        public NeighborsInfo(EDGETYPE edgeType, EdgeListWritable edgeListWritable){
            et = edgeType;
            edges = edgeListWritable;
        }

        @Override
        public Iterator<NeighborInfo> iterator() {
            return new Iterator<NeighborInfo>() {
                private int currentIndex = 0;
                
                @Override
                public boolean hasNext() {
                    return currentIndex < edges.size();
                }

                @Override
                public NeighborInfo next() {
                    return new NeighborInfo(et, edges.get(currentIndex));
                }

                @Override
                public void remove() {
                }
            };
        }
    }
        
    private static final long serialVersionUID = 1L;
    public static final NodeWritable EMPTY_NODE = new NodeWritable();

    private static final int SIZE_FLOAT = 4;

    private EdgeListWritable[] edges = { null, null, null, null };

    private PositionListWritable startReads; // first internalKmer in read
    private PositionListWritable endReads; // first internalKmer in read (but
                                           // internalKmer was flipped)

    private VKmerBytesWritable internalKmer;

    private float averageCoverage;
    
//    public boolean foundMe;
//    public String previous;
//    public int stepCount;
    // merge/update directions
    
    
    public NodeWritable() {
        
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            edges[e.get()] = new EdgeListWritable();
        }
        startReads = new PositionListWritable();
        endReads = new PositionListWritable();
        internalKmer = new VKmerBytesWritable(); // in graph construction - not
                                                 // set kmerlength
                                                 // Optimization: VKmer
        averageCoverage = 0;
//        this.foundMe = false;
//        this.previous = "";
//        this.stepCount = 0;
    }

    public NodeWritable(EdgeListWritable[] edges, PositionListWritable startReads, PositionListWritable endReads,
            VKmerBytesWritable kmer, float coverage) {
        this();
        setAsCopy(edges, startReads, endReads, kmer, coverage);
    }

    public NodeWritable(byte[] data, int offset) {
        this();
        setAsReference(data, offset);
    }
    
    public NodeWritable getNode(){
        return this;
    }
    
    public void setAsCopy(NodeWritable node) {
        setAsCopy(node.edges, node.startReads, node.endReads, node.internalKmer, node.averageCoverage);
    }

    public void setAsCopy(EdgeListWritable[] edges, PositionListWritable startReads, PositionListWritable endReads,
            VKmerBytesWritable kmer2, float coverage) {
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            this.edges[e.get()].setAsCopy(edges[e.get()]);
        }
        this.startReads.set(startReads);
        this.endReads.set(endReads);
        this.internalKmer.setAsCopy(kmer2);
        this.averageCoverage = coverage;
    }

    public void reset() {
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            edges[e.get()].reset();
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
    
    //This function works on only this case: in this DIR, vertex has and only has one EDGETYPE
    public EDGETYPE getNeighborEdgeType(DIR direction){
        if(getDegree(direction) != 1)
            throw new IllegalArgumentException("getEdgetypeFromDir is used on the case, in which the vertex has and only has one EDGETYPE!");
        EnumSet<EDGETYPE> ets = direction.edgeType(); 
        for(EDGETYPE et : ets){
            if(getEdgeList(et).size() > 0)
                return et;
        }
        throw new IllegalStateException("Programmer error: we shouldn't get here... Degree is 1 in " + direction + " but didn't find a an edge list > 1");
    }
    
    /**
     * Get this node's single neighbor in the given direction. Return null if there are multiple or no neighbors. 
     */
    public NeighborInfo getSingleNeighbor(DIR direction){
        if(getDegree(direction) != 1) {
            return null;
        }
        for(EDGETYPE et : direction.edgeType()) {
            if(getEdgeList(et).size() > 0) {
                return new NeighborInfo(et, getEdgeList(et).get(0));
            }
        }
        return null;
    }
    
    /**
     * Get this node's edgeType and edgeList in this given edgeType. Return null if there is no neighbor
     */
    public NeighborsInfo getNeighborsInfo(EDGETYPE et){
        if(getEdgeList(et).size() == 0)
            return null;
        return new NeighborsInfo(et, getEdgeList(et));
    }
    
    public EdgeListWritable getEdgeList(EDGETYPE edgeType) {
        return edges[edgeType.get() & EDGETYPE.MASK];
    }

    public void setEdgeList(EDGETYPE edgeType, EdgeListWritable edgeList) {
        this.edges[edgeType.get() & EDGETYPE.MASK].setAsCopy(edgeList);
    }
    
    public EdgeListWritable[] getEdges() {
        return edges;
    }

    public void setEdges(EdgeListWritable[] edges) {
        this.edges = edges;
    }

    public float getAverageCoverage() {
        return averageCoverage;
    }

    public void setAverageCoverage(float averageCoverage) {
        this.averageCoverage = averageCoverage;
    }

    /**
     * Update my coverage to be the average of this and other. Used when merging
     * paths.
     */
    public void mergeCoverage(NodeWritable other) {
        // sequence considered in the average doesn't include anything
        // overlapping with other kmers
        float adjustedLength = internalKmer.getKmerLetterLength() + other.internalKmer.getKmerLetterLength()
                - (KmerBytesWritable.getKmerLength() - 1) * 2;

        float myCount = (internalKmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() + 1) * averageCoverage;
        float otherCount = (other.internalKmer.getKmerLetterLength() - KmerBytesWritable.getKmerLength() + 1) * other.averageCoverage;
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
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            length += edges[e.get()].getLength();
        }
        length += startReads.getLength();
        length += endReads.getLength();
        length += internalKmer.getLength();
        length += SIZE_FLOAT; // avgCoverage
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
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            edges[e.get()].setAsCopy(data, curOffset);
            curOffset += edges[e.get()].getLength();
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
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            edges[e.get()].setAsReference(data, curOffset);
            curOffset += edges[e.get()].getLength();
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
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            edges[e.get()].write(out);
        }
        startReads.write(out);
        endReads.write(out);
        this.internalKmer.write(out);
        out.writeFloat(averageCoverage);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            edges[e.get()].readFields(in);
        }
        startReads.readFields(in);
        endReads.readFields(in);
        this.internalKmer.readFields(in);
        averageCoverage = in.readFloat();
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
        if (!(o instanceof NodeWritable))
            return false;

        NodeWritable nw = (NodeWritable) o;
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            if (!edges[e.get()].equals(nw.edges[e.get()]))
                return false;
        }
        
        return (averageCoverage == nw.averageCoverage && startReads.equals(nw.startReads) &&
                endReads.equals(nw.endReads) && internalKmer.equals(nw.internalKmer));
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            sbuilder.append(edges[e.get()].toString()).append('\t');
        }
        sbuilder.append("{5':" + startReads.toString() + ", ~5':" + endReads.toString() + "}").append('\t');
        sbuilder.append(internalKmer.toString()).append('\t');
        sbuilder.append(averageCoverage).append('x').append('}');
        return sbuilder.toString();
    }

    /**
     * merge this node with another node. If a flip is necessary, `other` will flip.
     * According to `dir`:
     * 1) kmers are concatenated/prepended/flipped
     * 2) coverage becomes a weighted average of the two spans
     * 3) startReads and endReads are merged and possibly flipped
     * 4) my edges are replaced with some subset of `other`'s edges
     * 
     * An error is raised when:
     * 1) non-overlapping kmers  // TODO
     * 2) `other` has degree > 1 towards me
     * 
     * @param dir
     *            : one of the DirectionFlag.DIR_*
     * @param other
     *            : the node to merge with. I should have a `dir` edge towards `other`
     */
    public void mergeWithNode(EDGETYPE edgeType, final NodeWritable other) {
        mergeEdges(edgeType, other);
        mergeStartAndEndReadIDs(edgeType, other);
        mergeCoverage(other);
        internalKmer.mergeWithKmerInDir(edgeType, KmerBytesWritable.lettersInKmer, other.internalKmer);
    }
    
    public void mergeWithNodeWithoutKmer(EDGETYPE edgeType, final NodeWritable other) {
        mergeEdges(edgeType, other);
        mergeStartAndEndReadIDs(edgeType, other);
        mergeCoverage(other);
    }
    
    public void mergeWithNodeWithoutKmer(final NodeWritable other) {
        EDGETYPE edgeType = EDGETYPE.FF;
        mergeEdges(edgeType, other);
        mergeStartAndEndReadIDs(edgeType, other);
        mergeCoverage(other);
    }

    /**
     * merge all metadata from `other` into this, as if `other` were the same node as this.
     * We don't touch the internal kmer but we do add edges, coverage, and start/end readids.
     */
    public void addFromNode(boolean flip, final NodeWritable other) {
        addEdges(flip, other);
        addCoverage(other);
        addStartAndEndReadIDs(flip, other);
    }

    /**
     * Add `other`'s readids to my own accounting for any differences in orientation and overall length.
     * differences in length will lead to relative offsets, where the incoming readids will be found in the
     * new sequence at the same relative position (e.g., 10% of the total length from 5' start).
     */
    private void addStartAndEndReadIDs(boolean flip, final NodeWritable other) {
        int otherLength = other.internalKmer.lettersInKmer;
        int thisLength = internalKmer.lettersInKmer;
        float lengthFactor = (float) thisLength / (float) otherLength;
        if (!flip) {
            // stream theirs in, adjusting to the new total length
            for (PositionWritable p : other.startReads) {
                startReads.append(p.getMateId(), p.getReadId(), (int) (p.getPosId() * lengthFactor));
            }
            for (PositionWritable p : other.endReads) {
                endReads.append(p.getMateId(), p.getReadId(), (int) (p.getPosId() * lengthFactor));
            }
        } else {
            int newOtherOffset = (int) ((otherLength - 1) * lengthFactor);
            // stream theirs in, offset and flipped
            for (PositionWritable p : other.startReads) {
                endReads.append(p.getMateId(), p.getReadId(), (int) (newOtherOffset - p.getPosId() * lengthFactor));
            }
            for (PositionWritable p : other.endReads) {
                startReads.append(p.getMateId(), p.getReadId(), (int) (newOtherOffset - p.getPosId() * lengthFactor));
            }
        }
    }
//
    /**
     * update my edge list
     */
    public void updateEdges(EDGETYPE deleteDir, VKmerBytesWritable toDelete, EDGETYPE updateDir, EDGETYPE replaceDir, NodeWritable other, boolean applyDelete){
        if(applyDelete)
            edges[deleteDir.get()].remove(toDelete);
        edges[updateDir.get()].unionUpdate(other.edges[replaceDir.get()]);
    }
    
    /**
     * merge my edge list (both kmers and readIDs) with those of `other`.  Assumes that `other` is doing the flipping, if any.
     */
    public void mergeEdges(EDGETYPE edgeType, NodeWritable other) {
        switch (edgeType) {
            case FF:
                if (outDegree() > 1)
                    throw new IllegalArgumentException("Illegal FF merge attempted! My outgoing degree is " + outDegree() + " in " + toString());
                if (other.inDegree() > 1)
                    throw new IllegalArgumentException("Illegal FF merge attempted! Other incoming degree is " + other.inDegree() + " in " + other.toString());
                edges[EDGETYPE.FF.get()].setAsCopy(other.edges[EDGETYPE.FF.get()]);
                edges[EDGETYPE.FR.get()].setAsCopy(other.edges[EDGETYPE.FR.get()]);
                break;
            case FR:
                if (outDegree() > 1)
                    throw new IllegalArgumentException("Illegal FR merge attempted! My outgoing degree is " + outDegree() + " in " + toString());
                if (other.outDegree() > 1)
                    throw new IllegalArgumentException("Illegal FR merge attempted! Other outgoing degree is " + other.outDegree() + " in " + other.toString());
                edges[EDGETYPE.FF.get()].setAsCopy(other.edges[EDGETYPE.RF.get()]);
                edges[EDGETYPE.FR.get()].setAsCopy(other.edges[EDGETYPE.RR.get()]);
                break;
            case RF:
                if (inDegree() > 1)
                    throw new IllegalArgumentException("Illegal RF merge attempted! My incoming degree is " + inDegree() + " in " + toString());
                if (other.inDegree() > 1)
                    throw new IllegalArgumentException("Illegal RF merge attempted! Other incoming degree is " + other.inDegree() + " in " + other.toString());
                edges[EDGETYPE.RF.get()].setAsCopy(other.edges[EDGETYPE.FF.get()]);
                edges[EDGETYPE.RR.get()].setAsCopy(other.edges[EDGETYPE.FR.get()]);
                break;
            case RR:
                if (inDegree() > 1)
                    throw new IllegalArgumentException("Illegal RR merge attempted! My incoming degree is " + inDegree() + " in " + toString());
                if (other.outDegree() > 1)
                    throw new IllegalArgumentException("Illegal RR merge attempted! Other outgoing degree is " + other.outDegree() + " in " + other.toString());
                edges[EDGETYPE.RF.get()].setAsCopy(other.edges[EDGETYPE.RF.get()]);
                edges[EDGETYPE.RR.get()].setAsCopy(other.edges[EDGETYPE.RR.get()]);
                break;
        }
    }

    private void addEdges(boolean flip, NodeWritable other) {
        if (!flip) {
            for (EDGETYPE e : EDGETYPE.values()) {
                edges[e.get()].unionUpdate(other.edges[e.get()]);
            }
        } else {
            edges[EDGETYPE.FF.get()].unionUpdate(other.edges[EDGETYPE.RF.get()]);
            edges[EDGETYPE.FR.get()].unionUpdate(other.edges[EDGETYPE.RR.get()]);
            edges[EDGETYPE.RF.get()].unionUpdate(other.edges[EDGETYPE.FF.get()]);
            edges[EDGETYPE.RR.get()].unionUpdate(other.edges[EDGETYPE.FR.get()]);
        }
    }

    private void mergeStartAndEndReadIDs(EDGETYPE edgeType, NodeWritable other) {
        int K = KmerBytesWritable.lettersInKmer;
        int otherLength = other.internalKmer.lettersInKmer;
        int thisLength = internalKmer.lettersInKmer;
        int newOtherOffset, newThisOffset;
        switch (edgeType) {
            case FF:
                newOtherOffset = thisLength - K + 1;
                // stream theirs in with my offset
                for (PositionWritable p : other.startReads) {
                    startReads.append(p.getMateId(), p.getReadId(), newOtherOffset + p.getPosId());
                }
                for (PositionWritable p : other.endReads) {
                    endReads.append(p.getMateId(), p.getReadId(), newOtherOffset + p.getPosId());
                }
                break;
            case FR:
                newOtherOffset = thisLength - K + 1 + otherLength - K;
                // stream theirs in, offset and flipped
                for (PositionWritable p : other.startReads) {
                    endReads.append(p.getMateId(), p.getReadId(), newOtherOffset + p.getPosId());
                }
                for (PositionWritable p : other.endReads) {
                    startReads.append(p.getMateId(), p.getReadId(), newOtherOffset + p.getPosId());
                }
                break;
            case RF:
                newThisOffset = otherLength - K + 1;
                newOtherOffset = otherLength - K;
                // shift my offsets (other is prepended)
                for (PositionWritable p : startReads) {
                    p.set(p.getMateId(), p.getReadId(), newThisOffset + p.getPosId());
                }
                for (PositionWritable p : endReads) {
                    p.set(p.getMateId(), p.getReadId(), newThisOffset + p.getPosId());
                }
                //stream theirs in, not offset (they are first now) but flipped
                for (PositionWritable p : other.startReads) {
                    endReads.append(p.getMateId(), p.getReadId(), newOtherOffset + p.getPosId());
                }
                for (PositionWritable p : other.endReads) {
                    startReads.append(p.getMateId(), p.getReadId(), newOtherOffset + p.getPosId());
                }
                break;
            case RR:
                newThisOffset = otherLength - K + 1;
                // shift my offsets (other is prepended)
                for (PositionWritable p : startReads) {
                    p.set(p.getMateId(), p.getReadId(), newThisOffset + p.getPosId());
                }
                for (PositionWritable p : endReads) {
                    p.set(p.getMateId(), p.getReadId(), newThisOffset + p.getPosId());
                }
                for (PositionWritable p : other.startReads) {
                    startReads.append(p);
                }
                for (PositionWritable p : other.endReads) {
                    endReads.append(p);
                }
                break;
        }
    }
    
    /**
     * Debug helper function to find the edge associated with the given kmer.
     * 
     * Note: may be very slow-- does a linear scan of all edges!
     */
    public Map.Entry<EDGETYPE, EdgeWritable> findEdge(final VKmerBytesWritable kmer) {
        for (EDGETYPE dir : EDGETYPE.values()) {
            for (EdgeWritable e : edges[dir.get()]) {
                if (e.getKey().equals(kmer))
                    return new AbstractMap.SimpleEntry<EDGETYPE, EdgeWritable>(dir, e);
            }
        }
        return null;
    }

    public int inDegree() {
        return edges[EDGETYPE.RR.get()].size() + edges[EDGETYPE.RF.get()].size();
    }

    public int outDegree() {
        return edges[EDGETYPE.FF.get()].size() + edges[EDGETYPE.FR.get()].size();
    }
    
    public int getDegree(DIR direction){
        return direction == DIR.REVERSE ? inDegree() : outDegree();
    }
    /*
     * Return if this node is a "path" compressible node, that is, it has an
     * in-degree and out-degree of 1
     */
    public boolean isPathNode() {
        return inDegree() == 1 && outDegree() == 1;
    }

    public boolean isSimpleOrTerminalPath() {
        return isPathNode() || (inDegree() == 0 && outDegree() == 1) || (inDegree() == 1 && outDegree() == 0);
    }
    
    public boolean isStartReadOrEndRead(){
        return startReads.getCountOfPosition() > 0 || endReads.getCountOfPosition() > 0;
    }

}
