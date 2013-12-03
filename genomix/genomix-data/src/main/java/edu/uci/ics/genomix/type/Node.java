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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.util.Marshal;

public class Node implements Writable, Serializable {

    public static final Logger LOG = Logger.getLogger(Node.class.getName());
    protected static boolean DEBUG = true;
    public static List<VKmer> problemKmers = new ArrayList<VKmer>();

    public enum READHEAD_ORIENTATION {
        UNFLIPPED((byte) 0),
        FLIPPED((byte) 1);

        private final byte val;

        private READHEAD_ORIENTATION(byte val) {
            this.val = val;
        }

        public final byte get() {
            return val;
        }

        public static READHEAD_ORIENTATION fromByte(byte b) {
            if (b == UNFLIPPED.val)
                return UNFLIPPED;
            else if (b == FLIPPED.val)
                return FLIPPED;
            return null;
        }
    }

    public static class NeighborInfo {
        public EDGETYPE et;
        public ReadIdSet readIds;
        public VKmer kmer;

        public NeighborInfo(EDGETYPE edgeType, VKmer kmer) {
            set(edgeType, kmer);
        }

        public void set(EDGETYPE edgeType, VKmer kmer) {
            this.et = edgeType;
            this.kmer = kmer;
        }

        public String toString() {
            StringBuilder sbuilder = new StringBuilder();
            sbuilder.append('{');
            sbuilder.append(kmer).append(" ").append(et);
            sbuilder.append('}');
            return sbuilder.toString();
        }
    }

    public static class NeighborsInfo implements Iterable<NeighborInfo> {
        public final EDGETYPE et;
        public final VKmerList edges;

        public NeighborsInfo(EDGETYPE et, VKmerList edges) {
            this.et = et;
            this.edges = edges;
        }

        @Override
        public Iterator<NeighborInfo> iterator() {
            return new Iterator<NeighborInfo>() {

                private Iterator<VKmer> it = edges.iterator();

                private NeighborInfo info = null;

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public NeighborInfo next() {
                    if (info == null) {
                        info = new NeighborInfo(et, it.next());
                    }
                    info.set(et, it.next());
                    return info;
                }

                @Override
                public void remove() {
                    it.remove();
                }
            };
        }
    }

    private static final long serialVersionUID = 1L;

    private VKmerList[] allEdges;
    private ReadHeadSet unflippedReadIds; // first Kmer in read
    private ReadHeadSet flippedReadIds; // first Kmer in read (but kmer was flipped)
    private VKmer internalKmer;

    private Float averageCoverage;

    public Node() {
        allEdges = new VKmerList[] { null, null, null, null };
        unflippedReadIds = null;
        flippedReadIds = null;
        internalKmer = null;
        averageCoverage = null;
    }

    public Node(VKmerList[] edges, ReadHeadSet unflippedReadIds, ReadHeadSet flippedReadIds, VKmer kmer, float coverage) {
        this();
        setAsCopy(edges, unflippedReadIds, flippedReadIds, kmer, coverage);
    }

    public Node(byte[] data, int offset) {
        this();
        setAsReference(data, offset);
    }

    public Node getCopyAsNode() {
        Node node = new Node();
        node.setAsCopy(this.allEdges, this.unflippedReadIds, this.flippedReadIds, this.internalKmer, this.averageCoverage);
        return node;
    }

    public void setAsCopy(Node node) {
        setAsCopy(node.allEdges, node.unflippedReadIds, node.flippedReadIds, node.internalKmer, node.averageCoverage);
    }

    public void setAsCopy(VKmerList[] edges, ReadHeadSet unflippedReadIds, ReadHeadSet flippedReadIds, VKmer kmer,
            Float coverage) {
        setAllEdges(edges);
        setUnflippedReadIds(unflippedReadIds);
        setFlippedReadIds(flippedReadIds);
        setInternalKmer(kmer);
        this.averageCoverage = coverage;
    }

    public void reset() {
        allEdges = new VKmerList[] { null, null, null, null };
        unflippedReadIds = null;
        flippedReadIds = null;
        internalKmer = null;
        averageCoverage = null;
    }

    public VKmer getInternalKmer() {
        if (internalKmer == null) {
            internalKmer = new VKmer();
        }
        return internalKmer;
    }

    public void setInternalKmer(VKmer kmer) {
        if (kmer == null) {
            this.internalKmer = null;
        } else {
            getInternalKmer().setAsCopy(kmer);
        }
    }

    public int getKmerLength() {
        return internalKmer.getKmerLetterLength();
    }

    // This function works on only this case: in this DIR, vertex has and only has one EDGETYPE
    public EDGETYPE getNeighborEdgeType(DIR direction) {
        if (degree(direction) != 1)
            throw new IllegalArgumentException(
                    "getEdgetypeFromDir is used on the case, in which the vertex has and only has one EDGETYPE!");
        EnumSet<EDGETYPE> ets = direction.edgeTypes();
        for (EDGETYPE et : ets) {
            if (allEdges[et.get()] != null && getEdges(et).size() > 0) {
                return et;
            }
        }
        throw new IllegalStateException("Programmer error: we shouldn't get here... Degree is 1 in " + direction
                + " but didn't find a an edge list > 1");
    }

    /**
     * Get this node's single neighbor in the given direction. Return null if there are multiple or no neighbors.
     */
    public NeighborInfo getSingleNeighbor(DIR direction) {
        if (degree(direction) != 1) {
            return null;
        }
        for (EDGETYPE et : direction.edgeTypes()) {
            if (allEdges[et.get()] != null && getEdges(et).size() > 0) {
                return new NeighborInfo(et, getEdges(et).getPosition(0));
            }
        }
        throw new IllegalStateException("Programmer error!!!");
    }

    /**
     * Get this node's edgeType and edges in this given edgeType. Return null if there is no neighbor
     */
    public NeighborsInfo getNeighborsInfo(EDGETYPE et) {
        if (allEdges[et.get()] == null || getEdges(et).size() == 0) {
            return null;
        }
        return new NeighborsInfo(et, getEdges(et));
    }

    public VKmerList getEdges(EDGETYPE edgeType) {
        if (allEdges[edgeType.get()] == null) {
            allEdges[edgeType.get()] = new VKmerList();
        }
        return allEdges[edgeType.get()];
    }

    public void setEdges(EDGETYPE edgeType, VKmerList edges) {
        if (edges == null) {
            allEdges[edgeType.get()] = null;
        } else {
            getEdges(edgeType).clear();
            getEdges(edgeType).setAsCopy(edges);
        }
    }

    public VKmerList[] getAllEdges() {
        return allEdges;
    }

    public void setAllEdges(VKmerList[] edges) {
        for (EDGETYPE et : EDGETYPE.values()) {
            setEdges(et, edges[et.get()]);
        }
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
    public void mergeCoverage(Node other) {
        // sequence considered in the average doesn't include anything
        // overlapping with other kmers
        float adjustedLength = internalKmer.getKmerLetterLength() + other.internalKmer.getKmerLetterLength()
                - (Kmer.getKmerLength() - 1) * 2;

        float myCount = (internalKmer.getKmerLetterLength() - Kmer.getKmerLength() + 1) * averageCoverage;
        float otherCount = (other.internalKmer.getKmerLetterLength() - Kmer.getKmerLength() + 1)
                * other.averageCoverage;
        averageCoverage = (myCount + otherCount) / adjustedLength;
    }

    /**
     * Update my coverage as if all the reads in other became my own
     */
    public void addCoverage(Node other) {
        float myAdjustedLength = internalKmer.getKmerLetterLength() - Kmer.getKmerLength() + 1;
        float otherAdjustedLength = other.internalKmer.getKmerLetterLength() - Kmer.getKmerLength() + 1;
        averageCoverage += other.averageCoverage * (otherAdjustedLength / myAdjustedLength);
    }

    public ReadHeadSet getUnflippedReadIds() {
        if (unflippedReadIds == null) {
            unflippedReadIds = new ReadHeadSet();
        }
        return unflippedReadIds;
    }

    public void setUnflippedReadIds(ReadHeadSet unflippedReadIds) {
        if (unflippedReadIds == null) {
            this.unflippedReadIds = null;
        } else {
            getUnflippedReadIds().clear();
            getUnflippedReadIds().addAll(unflippedReadIds);
        }
    }

    public ReadHeadSet getFlippedReadIds() {
        if (flippedReadIds == null) {
            flippedReadIds = new ReadHeadSet();
        }
        return flippedReadIds;
    }

    public void setFlippedReadIds(ReadHeadSet flippedReadIds) {
        if (flippedReadIds == null) {
            this.flippedReadIds = null;
        } else {
            getFlippedReadIds().clear();
            getFlippedReadIds().addAll(flippedReadIds);
        }
    }

    /**
     * Returns the length of the byte-array version of this node
     */
    public int getSerializedLength() {
        int length = Byte.SIZE / 8; // byte header
        for (EDGETYPE e : EDGETYPE.values()) {
            if (allEdges[e.get()] != null && allEdges[e.get()].size() > 0) {
                length += allEdges[e.get()].getLengthInBytes();
            }
        }
        if (unflippedReadIds != null && unflippedReadIds.size() > 0) {
            length += unflippedReadIds.getLengthInBytes();
        }
        if (flippedReadIds != null && flippedReadIds.size() > 0) {
            length += flippedReadIds.getLengthInBytes();
        }
        if (internalKmer != null && internalKmer.getKmerLetterLength() > 0) {
            length += internalKmer.getLength();
        }
        if (averageCoverage != null) {
            length += Float.SIZE / 8;
        }
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
        reset();
        byte activeFields = data[offset];
        offset += 1;
        for (EDGETYPE et : EDGETYPE.values()) {
            // et.get() is the index of the bit; if non-zero, we this edge is present in the stream
            if ((activeFields & (1 << et.get())) != 0) {
                getEdges(et).setAsCopy(data, offset);
                offset += allEdges[et.get()].getLengthInBytes();
            }
        }
        if ((activeFields & NODE_FIELDS.UNFLIPPED_READ_IDS) != 0) {
            getUnflippedReadIds().setAsCopy(data, offset);
            offset += unflippedReadIds.getLengthInBytes();
        }
        if ((activeFields & NODE_FIELDS.FLIPPED_READ_IDS) != 0) {
            getFlippedReadIds().setAsCopy(data, offset);
            offset += flippedReadIds.getLengthInBytes();
        }
        if ((activeFields & NODE_FIELDS.INTERNAL_KMER) != 0) {
            getInternalKmer().setAsCopy(data, offset);
            offset += internalKmer.getLength();
        }
        if ((activeFields & NODE_FIELDS.AVERAGE_COVERAGE) != 0) {
            averageCoverage = Marshal.getFloat(data, offset);
            offset += Float.SIZE / 8;
        }
    }

    public void setAsReference(byte[] data, int offset) {
        reset();
        byte activeFields = data[offset];
        offset += 1;
        for (EDGETYPE et : EDGETYPE.values()) {
            // et.get() is the index of the bit; if non-zero, we this edge is present in the stream
            if ((activeFields & (1 << et.get())) != 0) {
                getEdges(et).setAsReference(data, offset);
                offset += allEdges[et.get()].getLengthInBytes();
            }
        }
        if ((activeFields & NODE_FIELDS.UNFLIPPED_READ_IDS) != 0) {
            getUnflippedReadIds().setAsCopy(data, offset);
            offset += unflippedReadIds.getLengthInBytes();
        }
        if ((activeFields & NODE_FIELDS.FLIPPED_READ_IDS) != 0) {
            getFlippedReadIds().setAsCopy(data, offset);
            offset += flippedReadIds.getLengthInBytes();
        }
        if ((activeFields & NODE_FIELDS.INTERNAL_KMER) != 0) {
            getInternalKmer().setAsReference(data, offset);
            offset += internalKmer.getLength();
        }
        if ((activeFields & NODE_FIELDS.AVERAGE_COVERAGE) != 0) {
            averageCoverage = Marshal.getFloat(data, offset);
            offset += Float.SIZE / 8;
        }
    }

    public static void write(Node n, DataOutput out) throws IOException {
        out.writeByte(n.getActiveFields());
        for (EDGETYPE e : EDGETYPE.values()) {
            if (n.allEdges[e.get()] != null && n.allEdges[e.get()].size() > 0) {
                n.allEdges[e.get()].write(out);
            }
        }
        if (n.unflippedReadIds != null && n.unflippedReadIds.size() > 0) {
            n.unflippedReadIds.write(out);
        }
        if (n.flippedReadIds != null && n.flippedReadIds.size() > 0) {
            n.flippedReadIds.write(out);
        }
        if (n.internalKmer != null && n.internalKmer.getKmerLetterLength() > 0) {
            n.internalKmer.write(out);
        }
        if (n.averageCoverage != null) {
            out.writeFloat(n.averageCoverage);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        write(this, out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        byte activeFields = in.readByte();
        for (EDGETYPE et : EDGETYPE.values()) {
            // et.get() is the index of the bit; if non-zero, we this edge is present in the stream
            if ((activeFields & (1 << et.get())) != 0) {
                getEdges(et).readFields(in);
            }
        }
        if ((activeFields & NODE_FIELDS.UNFLIPPED_READ_IDS) != 0) {
            getUnflippedReadIds().readFields(in);
        }
        if ((activeFields & NODE_FIELDS.FLIPPED_READ_IDS) != 0) {
            getFlippedReadIds().readFields(in);
        }
        if ((activeFields & NODE_FIELDS.INTERNAL_KMER) != 0) {
            getInternalKmer().readFields(in);
        }
        if ((activeFields & NODE_FIELDS.AVERAGE_COVERAGE) != 0) {
            averageCoverage = in.readFloat();
        }
    }

    protected static class NODE_FIELDS {
        // bits 0-3 are for edge presence
        public static final int UNFLIPPED_READ_IDS = 1 << 4;
        public static final int FLIPPED_READ_IDS = 1 << 5;
        public static final int INTERNAL_KMER = 1 << 6;
        public static final int AVERAGE_COVERAGE = 1 << 7;
    }

    protected byte getActiveFields() {
        byte fields = 0;
        // bits 0-3 are for presence of edges
        for (EDGETYPE et : EDGETYPE.values()) {
            if (allEdges[et.get()] != null && allEdges[et.get()].size() > 0) {
                fields |= 1 << et.get();
            }
        }
        if (unflippedReadIds != null && unflippedReadIds.size() > 0) {
            fields |= NODE_FIELDS.UNFLIPPED_READ_IDS;
        }
        if (flippedReadIds != null && flippedReadIds.size() > 0) {
            fields |= NODE_FIELDS.FLIPPED_READ_IDS;
        }
        if (internalKmer != null && internalKmer.getKmerLetterLength() > 0) {
            fields |= NODE_FIELDS.INTERNAL_KMER;
        }
        if (averageCoverage != null) {
            fields |= NODE_FIELDS.AVERAGE_COVERAGE;
        }
        return fields;
    }

    public class SortByCoverage implements Comparator<Node> {
        @Override
        public int compare(Node left, Node right) {
            return Float.compare(left.averageCoverage, right.averageCoverage);
        }
    }

    @Override
    public int hashCode() {
        return this.internalKmer.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Node))
            return false;

        Node nw = (Node) o;
        for (EDGETYPE et : EDGETYPE.values()) {
            // If I'm null, return false if he's not null; otherwise, do a regular .equals
            if (allEdges[et.get()] == null ? nw.allEdges[et.get()] != null : allEdges[et.get()].equals(nw.allEdges[et.get()])) {
                return false;
            }
        }

        // in each case, if I'm null, he must also be null; otherwise, do regular .equals comparsion
        return ((averageCoverage == null ? nw.averageCoverage == null : averageCoverage == nw.averageCoverage) // coverage equality
                && (unflippedReadIds == null ? nw.unflippedReadIds == null : unflippedReadIds
                        .equals(nw.unflippedReadIds)) // unflipped equality
                && (flippedReadIds == null ? nw.flippedReadIds == null : flippedReadIds.equals(nw.flippedReadIds)) // flipped equality
        && (internalKmer == null ? nw.internalKmer == null : internalKmer.equals(nw.internalKmer) // internal kmer equality
        ));
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        for (EDGETYPE et : EDGETYPE.values()) {
            sbuilder.append(et + ":").append(allEdges[et.get()] == null ? "null" : allEdges[et.get()].toString())
                    .append('\t');
        }
        sbuilder.append("5':").append(unflippedReadIds == null ? "null" : unflippedReadIds.toString());
        sbuilder.append(", ~5':").append(flippedReadIds == null ? "null" : flippedReadIds.toString()).append('\t');
        sbuilder.append("kmer:").append(internalKmer == null ? "null" : internalKmer.toString()).append('\t');
        sbuilder.append("cov:").append(averageCoverage == null ? "null" : (averageCoverage + "x")).append('}');
        return sbuilder.toString();
    }

    /**
     * merge this node with another node. If a flip is necessary, `other` will flip.
     * According to `dir`:
     * 1) kmers are concatenated/prepended/flipped
     * 2) coverage becomes a weighted average of the two spans
     * 3) unFlippedReads and flippedReads are merged and possibly flipped
     * 4) my edges are replaced with some subset of `other`'s edges
     * An error is raised when:
     * 1) non-overlapping kmers // TODO
     * 2) `other` has degree > 1 towards me
     * 
     * @param dir
     *            : one of the DirectionFlag.DIR_*
     * @param other
     *            : the node to merge with. I should have a `dir` edge towards `other`
     */
    public void mergeWithNode(EDGETYPE edgeType, final Node other) {
        mergeEdges(edgeType, other);
        mergeUnflippedAndFlippedReadIDs(edgeType, other);
        mergeCoverage(other);
        internalKmer.mergeWithKmerInDir(edgeType, Kmer.getKmerLength(), other.internalKmer);
    }

    public void mergeWithNodeWithoutKmer(EDGETYPE edgeType, final Node other) {
        mergeEdges(edgeType, other);
        mergeUnflippedAndFlippedReadIDs(edgeType, other);
        mergeCoverage(other);
    }

    /**
     * merge all metadata from `other` into this, as if `other` were the same node as this.
     * We don't touch the internal kmer but we do add edges, coverage, and unflipped/flipped readids.
     */
    public void addFromNode(boolean flip, final Node other) {
        addEdges(flip, other);
        addCoverage(other);
        addUnflippedAndFlippedReadIds(flip, other);
    }

    /**
     * Add `other`'s readids to my own accounting for any differences in orientation and overall length.
     * differences in length will lead to relative offsets, where the incoming readids will be found in the
     * new sequence at the same relative position (e.g., 10% of the total length from 5' start).
     */
    protected void addUnflippedAndFlippedReadIds(boolean flip, final Node other) {
        int otherLength = other.internalKmer.lettersInKmer;
        int thisLength = internalKmer.lettersInKmer;
        float lengthFactor = (float) thisLength / (float) otherLength;
        if (!flip) {
            // stream theirs in, adjusting to the new total length

            if (other.unflippedReadIds != null) {
                for (ReadHeadInfo p : other.unflippedReadIds) {
                    getUnflippedReadIds().add(p.getMateId(), p.getReadId(),
                            (int) ((p.getOffset() + 1) * lengthFactor - lengthFactor), p.getThisReadSequence(),
                            p.getMateReadSequence());
                }
            }
            if (other.flippedReadIds != null) {
                for (ReadHeadInfo p : other.flippedReadIds) {
                    getFlippedReadIds().add(p.getMateId(), p.getReadId(),
                            (int) ((p.getOffset() + 1) * lengthFactor - lengthFactor), p.getThisReadSequence(),
                            p.getMateReadSequence());
                }
            }
        } else {
            //            int newOtherOffset = (int) ((otherLength - 1) * lengthFactor);
            // stream theirs in, offset and flipped
            int newPOffset;
            if (other.unflippedReadIds != null) {
                for (ReadHeadInfo p : other.unflippedReadIds) {
                    newPOffset = otherLength - 1 - p.getOffset();
                    getFlippedReadIds().add(p.getMateId(), p.getReadId(),
                            (int) ((newPOffset + 1) * lengthFactor - lengthFactor), p.getThisReadSequence(),
                            p.getMateReadSequence());
                }
            }
            if (other.flippedReadIds != null) {
                for (ReadHeadInfo p : other.flippedReadIds) {
                    newPOffset = otherLength - 1 - p.getOffset();
                    getUnflippedReadIds().add(p.getMateId(), p.getReadId(),
                            (int) ((newPOffset + 1) * lengthFactor - lengthFactor), p.getThisReadSequence(),
                            p.getMateReadSequence());
                }
            }
        }
    }

    /**
     * update my edge list
     */
    public void updateEdges(EDGETYPE deleteDir, VKmer toDelete, EDGETYPE updateDir, EDGETYPE replaceDir, Node other,
            boolean applyDelete) {
        if (applyDelete) {
            allEdges[deleteDir.get()].remove(toDelete);
        }
        if (other.allEdges[replaceDir.get()] != null) {
            getEdges(updateDir).unionUpdate(other.allEdges[replaceDir.get()]);
        }
    }

    /**
     * merge my edge list (both kmers and readIDs) with those of `other`. Assumes that `other` is doing the flipping, if any.
     */
    public void mergeEdges(EDGETYPE edgeType, Node other) {
        switch (edgeType) {
            case FF:
                if (outDegree() > 1)
                    throw new IllegalArgumentException("Illegal FF merge attempted! My outgoing degree is "
                            + outDegree() + " in " + toString());
                if (other.inDegree() > 1)
                    throw new IllegalArgumentException("Illegal FF merge attempted! Other incoming degree is "
                            + other.inDegree() + " in " + other.toString());
                if (other.allEdges[EDGETYPE.FF.get()] != null) {
                    getEdges(EDGETYPE.FF).setAsCopy(other.getEdges(EDGETYPE.FF));
                } else {
                    allEdges[EDGETYPE.FF.get()] = null;
                }
                if (other.allEdges[EDGETYPE.FR.get()] != null) {
                    getEdges(EDGETYPE.FR).setAsCopy(other.getEdges(EDGETYPE.FR));
                } else {
                    allEdges[EDGETYPE.FR.get()] = null;
                }
                break;
            case FR:
                if (outDegree() > 1)
                    throw new IllegalArgumentException("Illegal FR merge attempted! My outgoing degree is "
                            + outDegree() + " in " + toString());
                if (other.outDegree() > 1)
                    throw new IllegalArgumentException("Illegal FR merge attempted! Other outgoing degree is "
                            + other.outDegree() + " in " + other.toString());
                if (other.allEdges[EDGETYPE.RF.get()] != null) {
                    getEdges(EDGETYPE.FF).setAsCopy(other.getEdges(EDGETYPE.RF));
                } else {
                    allEdges[EDGETYPE.FF.get()] = null;
                }
                if (other.allEdges[EDGETYPE.RR.get()] != null) {
                    getEdges(EDGETYPE.FR).setAsCopy(other.getEdges(EDGETYPE.RR));
                } else {
                    allEdges[EDGETYPE.FR.get()] = null;
                }
                break;
            case RF:
                if (inDegree() > 1)
                    throw new IllegalArgumentException("Illegal RF merge attempted! My incoming degree is "
                            + inDegree() + " in " + toString());
                if (other.inDegree() > 1)
                    throw new IllegalArgumentException("Illegal RF merge attempted! Other incoming degree is "
                            + other.inDegree() + " in " + other.toString());
                if (other.allEdges[EDGETYPE.FF.get()] != null) {
                    getEdges(EDGETYPE.RF).setAsCopy(other.getEdges(EDGETYPE.FF));
                } else {
                    allEdges[EDGETYPE.RF.get()] = null;
                }
                if (other.allEdges[EDGETYPE.FR.get()] != null) {
                    getEdges(EDGETYPE.RR).setAsCopy(other.getEdges(EDGETYPE.FR));
                } else {
                    allEdges[EDGETYPE.RR.get()] = null;
                }
                break;
            case RR:
                if (inDegree() > 1)
                    throw new IllegalArgumentException("Illegal RR merge attempted! My incoming degree is "
                            + inDegree() + " in " + toString());
                if (other.outDegree() > 1)
                    throw new IllegalArgumentException("Illegal RR merge attempted! Other outgoing degree is "
                            + other.outDegree() + " in " + other.toString());
                if (other.allEdges[EDGETYPE.RF.get()] != null) {
                    getEdges(EDGETYPE.RF).setAsCopy(other.getEdges(EDGETYPE.RF));
                } else {
                    allEdges[EDGETYPE.RF.get()] = null;
                }
                if (other.allEdges[EDGETYPE.RR.get()] != null) {
                    getEdges(EDGETYPE.RR).setAsCopy(other.getEdges(EDGETYPE.RR));
                } else {
                    allEdges[EDGETYPE.RR.get()] = null;
                }
                break;
        }
    }

    protected void addEdges(boolean flip, Node other) {
        if (!flip) {
            for (EDGETYPE et : EDGETYPE.values()) {
                unionUpdateEdges(et, et, other.allEdges);
            }
        } else {
            unionUpdateEdges(EDGETYPE.FF, EDGETYPE.RF, other.allEdges);
            unionUpdateEdges(EDGETYPE.FR, EDGETYPE.RR, other.allEdges);
            unionUpdateEdges(EDGETYPE.RF, EDGETYPE.FF, other.allEdges);
            unionUpdateEdges(EDGETYPE.RR, EDGETYPE.FR, other.allEdges);
        }
    }

    private void unionUpdateEdges(EDGETYPE myET, EDGETYPE otherET, VKmerList[] otherEdges) {
        if (otherEdges[otherET.get()] != null) {
            getEdges(myET).unionUpdate(otherEdges[otherET.get()]);
        }
    }

    protected void mergeUnflippedAndFlippedReadIDs(EDGETYPE edgeType, Node other) {
        int K = Kmer.getKmerLength();
        int otherLength = other.internalKmer.lettersInKmer;
        int thisLength = internalKmer.lettersInKmer;
        int newOtherOffset, newThisOffset;
        switch (edgeType) {
            case FF:
                newOtherOffset = thisLength - K + 1;
                // stream theirs in with my offset
                if (other.unflippedReadIds != null) {
                    for (ReadHeadInfo p : other.unflippedReadIds) {
                        getUnflippedReadIds().add(p.getMateId(), p.getReadId(), newOtherOffset + p.getOffset(),
                                p.getThisReadSequence(), p.getMateReadSequence());
                    }
                }
                if (other.flippedReadIds != null) {
                    for (ReadHeadInfo p : other.flippedReadIds) {
                        getFlippedReadIds().add(p.getMateId(), p.getReadId(), newOtherOffset + p.getOffset(),
                                p.getThisReadSequence(), p.getMateReadSequence());
                    }
                }
                break;
            case FR:
                newOtherOffset = thisLength - K + otherLength;
                // stream theirs in, offset and flipped
                if (other.unflippedReadIds != null) {
                    for (ReadHeadInfo p : other.unflippedReadIds) {
                        getFlippedReadIds().add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset(),
                                p.getThisReadSequence(), p.getMateReadSequence());
                    }
                }
                if (other.flippedReadIds != null) {
                    for (ReadHeadInfo p : other.flippedReadIds) {
                        getUnflippedReadIds().add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset(),
                                p.getThisReadSequence(), p.getMateReadSequence());
                    }
                }
                break;
            case RF:
                newThisOffset = otherLength - K + 1;
                newOtherOffset = otherLength - 1;
                // shift my offsets (other is prepended)

                if (unflippedReadIds != null) {
                    for (ReadHeadInfo p : unflippedReadIds) {
                        p.set(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
                    }
                }
                if (flippedReadIds != null) {
                    for (ReadHeadInfo p : flippedReadIds) {
                        p.set(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
                    }
                }
                if (other.unflippedReadIds != null) {
                    for (ReadHeadInfo p : other.unflippedReadIds) {
                        getFlippedReadIds().add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset(),
                                p.getThisReadSequence(), p.getMateReadSequence());
                    }
                }
                if (other.flippedReadIds != null) {
                    for (ReadHeadInfo p : other.flippedReadIds) {
                        getUnflippedReadIds().add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset(),
                                p.getThisReadSequence(), p.getMateReadSequence());
                    }
                }
                break;
            case RR:
                newThisOffset = otherLength - K + 1;
                // shift my offsets (other is prepended)
                if (unflippedReadIds != null) {
                    for (ReadHeadInfo p : unflippedReadIds) {
                        p.set(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
                    }
                }
                if (flippedReadIds != null) {
                    for (ReadHeadInfo p : flippedReadIds) {
                        p.set(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
                    }
                }
                if (other.unflippedReadIds != null) {
                    for (ReadHeadInfo p : other.unflippedReadIds) {
                        getUnflippedReadIds().add(p);
                    }
                }
                if (other.flippedReadIds != null) {
                    for (ReadHeadInfo p : other.flippedReadIds) {
                        getFlippedReadIds().add(p);
                    }
                }
                break;
        }
    }

    /**
     * Debug helper function to find the edge associated with the given kmer, checking all directions. If the edge doesn't exist in any direction, returns null
     */
    public NeighborInfo findEdge(final VKmer kmer) {
        for (EDGETYPE et : EDGETYPE.values()) {
            if (allEdges[et.get()] != null && allEdges[et.get()].contains(kmer)) {
                return new NeighborInfo(et, kmer);
            }
        }
        return null;
    }

    public int degree(DIR direction) {
        int totalDegree = 0;
        for (EDGETYPE et : DIR.edgeTypesInDir(direction)) {
            if (allEdges[et.get()] != null) {
                totalDegree += allEdges[et.get()].size();
            }
        }
        return totalDegree;
    }

    public int inDegree() {
        return degree(DIR.REVERSE);
    }

    public int outDegree() {
        return degree(DIR.FORWARD);
    }

    public int totalDegree() {
        return degree(DIR.FORWARD) + degree(DIR.REVERSE);
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

    public boolean isUnflippedOrFlippedReadIds() {
        return (unflippedReadIds != null && unflippedReadIds.size() > 0)
                || (flippedReadIds != null && flippedReadIds.size() > 0);
    }
}
