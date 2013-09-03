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
import java.util.Comparator;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.data.Marshal;

public class NodeWritable implements WritableComparable<NodeWritable>, Serializable {

    private static final long serialVersionUID = 1L;
    public static final NodeWritable EMPTY_NODE = new NodeWritable();

    private static final int SIZE_FLOAT = 4;

    private EdgeListWritable[] edges = { null, null, null, null };

    private PositionListWritable startReads; // first internalKmer in read
    private PositionListWritable endReads; // first internalKmer in read (but
                                           // internalKmer was flipped)

    private VKmerBytesWritable internalKmer;

    private float averageCoverage;

    // merge/update directions
    public static class DirectionFlag {
        public static final byte DIR_FF = 0b00 << 0;
        public static final byte DIR_FR = 0b01 << 0;
        public static final byte DIR_RF = 0b10 << 0;
        public static final byte DIR_RR = 0b11 << 0;
        public static final byte DIR_MASK = 0b11 << 0;
        public static final byte DIR_CLEAR = 0b1111100 << 0;

        public static final byte[] values = { DIR_FF, DIR_FR, DIR_RF, DIR_RR };
    }
    
    public static class IncomingListFlag {
        public static final byte DIR_RF = 0b10 << 0;
        public static final byte DIR_RR = 0b11 << 0;

        public static final byte[] values = {DIR_RF, DIR_RR };
    }
    
    public static class OutgoingListFlag {
        public static final byte DIR_FF = 0b00 << 0;
        public static final byte DIR_FR = 0b01 << 0;

        public static final byte[] values = {DIR_FF, DIR_FR };
    }
    
    public NodeWritable() {
        for (byte d : DirectionFlag.values) {
            edges[d] = new EdgeListWritable();
        }
        startReads = new PositionListWritable();
        endReads = new PositionListWritable();
        internalKmer = new VKmerBytesWritable(); // in graph construction - not
                                                 // set kmerlength
                                                 // Optimization: VKmer
        averageCoverage = 0;
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
        for (byte d : DirectionFlag.values) {
            this.edges[d].setAsCopy(edges[d]);
        }
        this.startReads.set(startReads);
        this.endReads.set(endReads);
        this.internalKmer.setAsCopy(kmer2);
        this.averageCoverage = coverage;
    }

    public void reset() {
        for (byte d : DirectionFlag.values) {
            edges[d].reset();
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

    public EdgeListWritable getEdgeList(byte dir) {
        return edges[dir & DirectionFlag.DIR_MASK];
    }

    public void setEdgeList(byte dir, EdgeListWritable edgeList) {
        this.edges[dir & DirectionFlag.DIR_MASK].setAsCopy(edgeList);
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
        for (byte d : DirectionFlag.values) {
            length += edges[d].getLength();
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
        for (byte d : DirectionFlag.values) {
            edges[d].setAsCopy(data, curOffset);
            curOffset += edges[d].getLength();
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
        for (byte d : DirectionFlag.values) {
            edges[d].setAsReference(data, curOffset);
            curOffset += edges[d].getLength();
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
        for (byte d : DirectionFlag.values) {
            edges[d].write(out);
        }
        startReads.write(out);
        endReads.write(out);
        this.internalKmer.write(out);
        out.writeFloat(averageCoverage);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        for (byte d : DirectionFlag.values) {
            edges[d].readFields(in);
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
        if (!(o instanceof NodeWritable))
            return false;

        NodeWritable nw = (NodeWritable) o;
        for (byte d : DirectionFlag.values) {
            if (!edges[d].equals(nw.edges[d]))
                return false;
        }
        
        return (averageCoverage == nw.averageCoverage && startReads.equals(nw.startReads) &&
                endReads.equals(nw.endReads) && internalKmer.equals(nw.internalKmer));
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        for (byte d : DirectionFlag.values) {
            sbuilder.append(edges[d].toString()).append('\t');
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
    public void mergeWithNode(byte dir, final NodeWritable other) {
        mergeEdges(dir, other);
        mergeStartAndEndReadIDs(dir, other);
        mergeCoverage(other);
        internalKmer.mergeWithKmerInDir(dir, KmerBytesWritable.lettersInKmer, other.internalKmer);
    }
    
    public void mergeWithNodeWithoutKmer(byte dir, final NodeWritable other) {
        mergeEdges(dir, other);
        mergeStartAndEndReadIDs(dir, other);
        mergeCoverage(other);
    }
    
    public void mergeWithNodeWithoutKmer(final NodeWritable other) {
        byte dir = DirectionFlag.DIR_FF;
        mergeEdges(dir, other);
        mergeStartAndEndReadIDs(dir, other);
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
        if (flip) {
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

    /**
     * update my edge list
     */
    public void updateEdges(byte deleteDir, VKmerBytesWritable toDelete, byte updateDir, byte replaceDir, NodeWritable other, boolean applyDelete){
        if(applyDelete)
            edges[deleteDir].remove(toDelete);
        switch (replaceDir) {
            case DirectionFlag.DIR_FF:
            case DirectionFlag.DIR_FR:
                edges[updateDir].unionUpdate(other.edges[DirectionFlag.DIR_RF]);
                edges[updateDir].unionUpdate(other.edges[DirectionFlag.DIR_RR]);
                break;
            case DirectionFlag.DIR_RF:
            case DirectionFlag.DIR_RR:
                edges[updateDir].unionUpdate(other.edges[DirectionFlag.DIR_FF]);
                edges[updateDir].unionUpdate(other.edges[DirectionFlag.DIR_FR]);
                break;
        }
//        switch (updateDir) {
//            case DirectionFlag.DIR_FF:
//                edges[DirectionFlag.DIR_FF].unionUpdate(other.edges[DirectionFlag.DIR_FF]);
//                edges[DirectionFlag.DIR_FR].unionUpdate(other.edges[DirectionFlag.DIR_FR]);
//                break;
//            case DirectionFlag.DIR_FR:
//                edges[DirectionFlag.DIR_FF].unionUpdate(other.edges[DirectionFlag.DIR_RF]);
//                edges[DirectionFlag.DIR_FR].unionUpdate(other.edges[DirectionFlag.DIR_RR]);
//                break;
//            case DirectionFlag.DIR_RF:
//                edges[DirectionFlag.DIR_RF].unionUpdate(other.edges[DirectionFlag.DIR_FF]);
//                edges[DirectionFlag.DIR_RR].unionUpdate(other.edges[DirectionFlag.DIR_FR]);
//                break;
//            case DirectionFlag.DIR_RR:
//                edges[DirectionFlag.DIR_RF].unionUpdate(other.edges[DirectionFlag.DIR_RF]);
//                edges[DirectionFlag.DIR_RR].unionUpdate(other.edges[DirectionFlag.DIR_RR]);
//                break;
//        }
    }
    
    /**
     * merge my edge list (both kmers and readIDs) with those of `other`.  Assumes that `other` is doing the flipping, if any.
     */
    public void mergeEdges(byte dir, NodeWritable other) {
        switch (dir & DirectionFlag.DIR_MASK) {
            case DirectionFlag.DIR_FF:
                if (outDegree() > 1)
                    throw new IllegalArgumentException("Illegal FF merge attempted! My outgoing degree is " + outDegree() + " in " + toString());
                if (other.inDegree() > 1)
                    throw new IllegalArgumentException("Illegal FF merge attempted! Other incoming degree is " + other.inDegree() + " in " + other.toString());
                edges[DirectionFlag.DIR_FF].setAsCopy(other.edges[DirectionFlag.DIR_FF]);
                edges[DirectionFlag.DIR_FR].setAsCopy(other.edges[DirectionFlag.DIR_FR]);
                break;
            case DirectionFlag.DIR_FR:
                if (outDegree() > 1)
                    throw new IllegalArgumentException("Illegal FR merge attempted! My outgoing degree is " + outDegree() + " in " + toString());
                if (other.outDegree() > 1)
                    throw new IllegalArgumentException("Illegal FR merge attempted! Other outgoing degree is " + other.outDegree() + " in " + other.toString());
                edges[DirectionFlag.DIR_FF].setAsCopy(other.edges[DirectionFlag.DIR_RF]);
                edges[DirectionFlag.DIR_FR].setAsCopy(other.edges[DirectionFlag.DIR_RR]);
                break;
            case DirectionFlag.DIR_RF:
                if (inDegree() > 1)
                    throw new IllegalArgumentException("Illegal RF merge attempted! My incoming degree is " + inDegree() + " in " + toString());
                if (other.inDegree() > 1)
                    throw new IllegalArgumentException("Illegal RF merge attempted! Other incoming degree is " + other.inDegree() + " in " + other.toString());
                edges[DirectionFlag.DIR_RF].setAsCopy(other.edges[DirectionFlag.DIR_FF]);
                edges[DirectionFlag.DIR_RR].setAsCopy(other.edges[DirectionFlag.DIR_FR]);
                break;
            case DirectionFlag.DIR_RR:
                if (inDegree() > 1)
                    throw new IllegalArgumentException("Illegal RR merge attempted! My incoming degree is " + inDegree() + " in " + toString());
                if (other.outDegree() > 1)
                    throw new IllegalArgumentException("Illegal RR merge attempted! Other outgoing degree is " + other.outDegree() + " in " + other.toString());
                edges[DirectionFlag.DIR_RF].setAsCopy(other.edges[DirectionFlag.DIR_RF]);
                edges[DirectionFlag.DIR_RR].setAsCopy(other.edges[DirectionFlag.DIR_RR]);
                break;
        }
    }

    private void addEdges(boolean flip, NodeWritable other) {
        if (!flip) {
            for (byte d : DirectionFlag.values) {
                edges[d].unionUpdate(other.edges[d]);
            }
        } else {
            edges[DirectionFlag.DIR_FF].unionUpdate(other.edges[DirectionFlag.DIR_RF]);
            edges[DirectionFlag.DIR_FR].unionUpdate(other.edges[DirectionFlag.DIR_RR]);
            edges[DirectionFlag.DIR_RF].unionUpdate(other.edges[DirectionFlag.DIR_FF]);
            edges[DirectionFlag.DIR_RR].unionUpdate(other.edges[DirectionFlag.DIR_FR]);
        }
    }

    private void mergeStartAndEndReadIDs(byte dir, NodeWritable other) {
        int K = KmerBytesWritable.lettersInKmer;
        int otherLength = other.internalKmer.lettersInKmer;
        int thisLength = internalKmer.lettersInKmer;
        int newOtherOffset, newThisOffset;
        switch (dir & DirectionFlag.DIR_MASK) {
            case DirectionFlag.DIR_FF:
                newOtherOffset = thisLength - K + 1;
                // stream theirs in with my offset
                for (PositionWritable p : other.startReads) {
                    startReads.append(p.getMateId(), p.getReadId(), newOtherOffset + p.getPosId());
                }
                for (PositionWritable p : other.endReads) {
                    endReads.append(p.getMateId(), p.getReadId(), newOtherOffset + p.getPosId());
                }
                break;
            case DirectionFlag.DIR_FR:
                newOtherOffset = thisLength - K + 1 + otherLength - K;
                // stream theirs in, offset and flipped
                for (PositionWritable p : other.startReads) {
                    endReads.append(p.getMateId(), p.getReadId(), newOtherOffset + p.getPosId());
                }
                for (PositionWritable p : other.endReads) {
                    startReads.append(p.getMateId(), p.getReadId(), newOtherOffset + p.getPosId());
                }
                break;
            case DirectionFlag.DIR_RF:
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
            case DirectionFlag.DIR_RR:
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

    public int inDegree() {
        return edges[DirectionFlag.DIR_RR].getCountOfPosition() + edges[DirectionFlag.DIR_RF].getCountOfPosition();
    }

    public int outDegree() {
        return edges[DirectionFlag.DIR_FF].getCountOfPosition() + edges[DirectionFlag.DIR_FR].getCountOfPosition();
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