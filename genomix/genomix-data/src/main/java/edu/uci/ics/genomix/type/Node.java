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
import java.util.Map.Entry;
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

        public NeighborInfo(EDGETYPE edgeType, VKmer kmer, ReadIdSet readIds) {
            set(edgeType, kmer, readIds);
        }

        public NeighborInfo(EDGETYPE edgeType, Entry<VKmer, ReadIdSet> edge) {
            set(edgeType, edge.getKey(), edge.getValue());
        }

        public void set(EDGETYPE edgeType, Entry<VKmer, ReadIdSet> edge) {
            set(edgeType, edge.getKey(), edge.getValue());
        }

        public void set(EDGETYPE edgeType, VKmer kmer, ReadIdSet readIds) {
            this.et = edgeType;
            this.kmer = kmer;
            this.readIds = readIds;
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
        public final EdgeMap edges;

        public NeighborsInfo(EDGETYPE edgeType, EdgeMap edgeList) {
            et = edgeType;
            edges = edgeList;
        }

        @Override
        public Iterator<NeighborInfo> iterator() {
            return new Iterator<NeighborInfo>() {

                private Iterator<Entry<VKmer, ReadIdSet>> it = edges.entrySet().iterator();

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
    public static final Node EMPTY_NODE = new Node();

    private static final int SIZE_FLOAT = 4;

    private EdgeMap[] edges = { null, null, null, null };

    private ReadHeadSet unflippedReadIds; // first internalKmer in read
    private ReadHeadSet flippedReadIds; // first internalKmer in read (but
    // internalKmer was flipped)

    private VKmer internalKmer;

    private float averageCoverage;

    //    public boolean foundMe;
    //    public String previous;
    //    public int stepCount;
    // merge/update directions

    public Node() {

        for (EDGETYPE e : EDGETYPE.values()) {
            edges[e.get()] = new EdgeMap();
        }
        unflippedReadIds = new ReadHeadSet();
        flippedReadIds = new ReadHeadSet();
        internalKmer = new VKmer(); // in graph construction - not
                                    // set kmerlength
                                    // Optimization: VKmer
        averageCoverage = 0;
        //        this.foundMe = false;
        //        this.previous = "";
        //        this.stepCount = 0;
    }

    public Node(EdgeMap[] edges, ReadHeadSet unflippedReadIds, ReadHeadSet flippedReadIds, VKmer kmer, float coverage) {
        this();
        setAsCopy(edges, unflippedReadIds, flippedReadIds, kmer, coverage);
    }

    public Node(byte[] data, int offset) {
        this();
        setAsReference(data, offset);
    }


    public Node getNode() {
        return this;
    }

    public Node getCopyAsNode() {
        Node node = new Node();
        node.setAsCopy(this.edges, this.unflippedReadIds, this.flippedReadIds, this.internalKmer, this.averageCoverage);
        return node;
    }

    public void setAsCopy(Node node) {
        setAsCopy(node.edges, node.unflippedReadIds, node.flippedReadIds, node.internalKmer, node.averageCoverage);
    }

    public void setAsCopy(EdgeMap[] edges, ReadHeadSet unflippedReadIds, ReadHeadSet flippedReadIds, VKmer kmer,
            float coverage) {
        for (EDGETYPE e : EDGETYPE.values()) {
            this.edges[e.get()].setAsCopy(edges[e.get()]);
        }
        this.unflippedReadIds.clear();
        this.unflippedReadIds.addAll(unflippedReadIds);
        this.flippedReadIds.clear();
        this.flippedReadIds.addAll(flippedReadIds);
        this.internalKmer.setAsCopy(kmer);
        this.averageCoverage = coverage;
    }

    public void reset() {
        for (EDGETYPE e : EDGETYPE.values()) {
            edges[e.get()].clear();
        }
        unflippedReadIds.clear();
        flippedReadIds.clear();
        internalKmer.reset(0);
        averageCoverage = 0;
    }

    public VKmer getInternalKmer() {
        return internalKmer;
    }

    public void setInternalKmer(VKmer internalKmer) {
        this.internalKmer.setAsCopy(internalKmer);
    }

    public int getKmerLength() {
        return internalKmer.getKmerLetterLength();
    }

    //This function works on only this case: in this DIR, vertex has and only has one EDGETYPE
    public EDGETYPE getNeighborEdgeType(DIR direction) {
        if (degree(direction) != 1)
            throw new IllegalArgumentException(
                    "getEdgetypeFromDir is used on the case, in which the vertex has and only has one EDGETYPE!");
        EnumSet<EDGETYPE> ets = direction.edgeTypes();
        for (EDGETYPE et : ets) {
            if (getEdgeMap(et).size() > 0)
                return et;
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
            if (getEdgeMap(et).size() > 0) {
                return new NeighborInfo(et, getEdgeMap(et).firstEntry());
            }
        }
        throw new IllegalStateException("Programmer error!!!");
    }

    /**
     * Get this node's edgeType and edgeList in this given edgeType. Return null if there is no neighbor
     */
    public NeighborsInfo getNeighborsInfo(EDGETYPE et) {
        if (getEdgeMap(et).size() == 0)
            return null;
        return new NeighborsInfo(et, getEdgeMap(et));
    }

    public EdgeMap getEdgeMap(EDGETYPE edgeType) {
        return edges[edgeType.get()];
    }

    public void setEdgeMap(EDGETYPE edgeType, EdgeMap edgeMap) {
        this.edges[edgeType.get()].setAsCopy(edgeMap);
    }

    public EdgeMap[] getEdges() {
        return edges;
    }

    public void setEdges(EdgeMap[] edges) {
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
        return unflippedReadIds;
    }

    // TODO rename the function
    public void setUnflippedReadIds(ReadHeadSet unflippedReadIds) {
        this.unflippedReadIds.clear();
        this.unflippedReadIds.addAll(unflippedReadIds);
    }

    public ReadHeadSet getFlippedReadIds() {
        return flippedReadIds;
    }

    public void setFlippedReadIds(ReadHeadSet flippedReadIds) {
        this.flippedReadIds.clear();
        this.flippedReadIds.addAll(flippedReadIds);
    }

    /**
     * Returns the length of the byte-array version of this node
     */
    public int getSerializedLength() {
        int length = 0;
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            length += edges[e.get()].getLengthInBytes();
        }
        length += unflippedReadIds.getLengthInBytes();
        length += flippedReadIds.getLengthInBytes();
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
            curOffset += edges[e.get()].getLengthInBytes();
        }
        unflippedReadIds.setAsCopy(data, curOffset);
        curOffset += unflippedReadIds.getLengthInBytes();
        flippedReadIds.setAsCopy(data, curOffset);
        curOffset += flippedReadIds.getLengthInBytes();
        internalKmer.setAsCopy(data, curOffset);
        curOffset += internalKmer.getLength();
        averageCoverage = Marshal.getFloat(data, curOffset);
    }

    public void setAsReference(byte[] data, int offset) {
        int curOffset = offset;
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            edges[e.get()].setAsReference(data, curOffset);
            curOffset += edges[e.get()].getLengthInBytes();
        }
        unflippedReadIds.setAsCopy(data, curOffset);
        curOffset += unflippedReadIds.getLengthInBytes();
        flippedReadIds.setAsCopy(data, curOffset);
        curOffset += flippedReadIds.getLengthInBytes();

        internalKmer.setAsReference(data, curOffset);
        curOffset += internalKmer.getLength();
        averageCoverage = Marshal.getFloat(data, curOffset);
    }

    public static void write(Node n, DataOutput out) throws IOException {
        for (EDGETYPE e : EDGETYPE.values()) {
            n.edges[e.get()].write(out);
        }
        n.unflippedReadIds.write(out);
        n.flippedReadIds.write(out);
        n.internalKmer.write(out);
        out.writeFloat(n.averageCoverage);

        //        if (DEBUG) {
        //            boolean verbose = false;
        //            for (VKmer problemKmer : problemKmers) {
        //                verbose |= n.findEdge(problemKmer) != null;
        //            }
        //            if (verbose) {
        //                LOG.fine("write: " + n.toString());
        //            }
        //        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        write(this, out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        for (EDGETYPE e : EDGETYPE.values()) {
            edges[e.get()].readFields(in);
        }
        unflippedReadIds.readFields(in);
        flippedReadIds.readFields(in);
        this.internalKmer.readFields(in);
        averageCoverage = in.readFloat();

        if (DEBUG) {
            boolean verbose = false;
            for (VKmer problemKmer : problemKmers) {
                verbose |= findEdge(problemKmer) != null;
            }
            if (verbose) {
                LOG.fine("readFields: " + toString());
            }
        }
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
        for (EDGETYPE e : EnumSet.allOf(EDGETYPE.class)) {
            if (!edges[e.get()].equals(nw.edges[e.get()]))
                return false;
        }

        return (averageCoverage == nw.averageCoverage && unflippedReadIds.equals(nw.unflippedReadIds)
                && flippedReadIds.equals(nw.flippedReadIds) && internalKmer.equals(nw.internalKmer));
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        for (EDGETYPE e : EDGETYPE.values()) {
            sbuilder.append(e + ":").append(edges[e.get()].toString()).append('\t');
        }
        sbuilder.append("5':" + unflippedReadIds.toString() + ", ~5':" + flippedReadIds.toString()).append('\t');
        sbuilder.append("kmer:" + internalKmer.toString()).append('\t');
        sbuilder.append("cov:" + averageCoverage).append('x').append('}');
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
        internalKmer.mergeWithKmerInDir(edgeType, Kmer.lettersInKmer, other.internalKmer);
    }

    public void mergeWithNodeWithoutKmer(EDGETYPE edgeType, final Node other) {
        mergeEdges(edgeType, other);
        mergeUnflippedAndFlippedReadIDs(edgeType, other);
        mergeCoverage(other);
    }

    public void mergeWithNodeWithoutKmer(final Node other) {
        EDGETYPE edgeType = EDGETYPE.FF;
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
            for (ReadHeadInfo p : other.unflippedReadIds) {
                unflippedReadIds.add(p.getMateId(), p.getReadId(),
                        (int) ((p.getOffset() + 1) * lengthFactor - lengthFactor));
            }
            for (ReadHeadInfo p : other.flippedReadIds) {
                flippedReadIds.add(p.getMateId(), p.getReadId(),
                        (int) ((p.getOffset() + 1) * lengthFactor - lengthFactor));
            }
        } else {
//            int newOtherOffset = (int) ((otherLength - 1) * lengthFactor);
            // stream theirs in, offset and flipped
            int newPOffset;
            for (ReadHeadInfo p : other.unflippedReadIds) {
                newPOffset = otherLength - 1 - p.getOffset();
                flippedReadIds
                        .add(p.getMateId(), p.getReadId(), (int) ((newPOffset + 1) * lengthFactor - lengthFactor));
            }
            for (ReadHeadInfo p : other.flippedReadIds) {
                newPOffset = otherLength - 1 - p.getOffset();
                unflippedReadIds.add(p.getMateId(), p.getReadId(),
                        (int) ((newPOffset + 1) * lengthFactor - lengthFactor));
            }
        }
    }

    //
    /**
     * update my edge list
     */
    public void updateEdges(EDGETYPE deleteDir, VKmer toDelete, EDGETYPE updateDir, EDGETYPE replaceDir, Node other,
            boolean applyDelete) {
        if (applyDelete)
            edges[deleteDir.get()].remove(toDelete);
        edges[updateDir.get()].unionUpdate(other.edges[replaceDir.get()]);
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
                edges[EDGETYPE.FF.get()].setAsCopy(other.edges[EDGETYPE.FF.get()]);
                edges[EDGETYPE.FR.get()].setAsCopy(other.edges[EDGETYPE.FR.get()]);
                break;
            case FR:
                if (outDegree() > 1)
                    throw new IllegalArgumentException("Illegal FR merge attempted! My outgoing degree is "
                            + outDegree() + " in " + toString());
                if (other.outDegree() > 1)
                    throw new IllegalArgumentException("Illegal FR merge attempted! Other outgoing degree is "
                            + other.outDegree() + " in " + other.toString());
                edges[EDGETYPE.FF.get()].setAsCopy(other.edges[EDGETYPE.RF.get()]);
                edges[EDGETYPE.FR.get()].setAsCopy(other.edges[EDGETYPE.RR.get()]);
                break;
            case RF:
                if (inDegree() > 1)
                    throw new IllegalArgumentException("Illegal RF merge attempted! My incoming degree is "
                            + inDegree() + " in " + toString());
                if (other.inDegree() > 1)
                    throw new IllegalArgumentException("Illegal RF merge attempted! Other incoming degree is "
                            + other.inDegree() + " in " + other.toString());
                edges[EDGETYPE.RF.get()].setAsCopy(other.edges[EDGETYPE.FF.get()]);
                edges[EDGETYPE.RR.get()].setAsCopy(other.edges[EDGETYPE.FR.get()]);
                break;
            case RR:
                if (inDegree() > 1)
                    throw new IllegalArgumentException("Illegal RR merge attempted! My incoming degree is "
                            + inDegree() + " in " + toString());
                if (other.outDegree() > 1)
                    throw new IllegalArgumentException("Illegal RR merge attempted! Other outgoing degree is "
                            + other.outDegree() + " in " + other.toString());
                edges[EDGETYPE.RF.get()].setAsCopy(other.edges[EDGETYPE.RF.get()]);
                edges[EDGETYPE.RR.get()].setAsCopy(other.edges[EDGETYPE.RR.get()]);
                break;
        }
    }

    protected void addEdges(boolean flip, Node other) {
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
    protected void mergeUnflippedAndFlippedReadIDs(EDGETYPE edgeType, Node other) {
        int K = Kmer.lettersInKmer;
        int otherLength = other.internalKmer.lettersInKmer;
        int thisLength = internalKmer.lettersInKmer;
        int newOtherOffset, newThisOffset;
        switch (edgeType) {
            case FF:
                newOtherOffset = thisLength - K + 1;
                // stream theirs in with my offset
                for (ReadHeadInfo p : other.unflippedReadIds) {
                    unflippedReadIds.add(p.getMateId(), p.getReadId(), newOtherOffset + p.getOffset());
                }
                for (ReadHeadInfo p : other.flippedReadIds) {
                    flippedReadIds.add(p.getMateId(), p.getReadId(), newOtherOffset + p.getOffset());
                }
                break;
            case FR:
                newOtherOffset = thisLength - K  + otherLength;
                // stream theirs in, offset and flipped
                for (ReadHeadInfo p : other.unflippedReadIds) {
                    flippedReadIds.add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset());
                }
                for (ReadHeadInfo p : other.flippedReadIds) {
                    unflippedReadIds.add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset());
                }
                break;
            case RF:
                newThisOffset = otherLength - K + 1;
                newOtherOffset = otherLength - 1;
                // shift my offsets (other is prepended)
                for (ReadHeadInfo p : unflippedReadIds) {
                    p.set(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
                }
                for (ReadHeadInfo p : flippedReadIds) {
                    p.set(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
                }
//                System.out.println(startReads.size());
//                System.out.println(endReads.size());
                //stream theirs in, not offset (they are first now) but flipped
                for (ReadHeadInfo p : other.unflippedReadIds) {
                    flippedReadIds.add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset());
                }
                for (ReadHeadInfo p : other.flippedReadIds) {
                    unflippedReadIds.add(p.getMateId(), p.getReadId(), newOtherOffset - p.getOffset());
                }
                break;
            case RR:
                newThisOffset = otherLength - K + 1;
                // shift my offsets (other is prepended)
                for (ReadHeadInfo p : unflippedReadIds) {
                    p.set(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
                }
                for (ReadHeadInfo p : flippedReadIds) {
                    p.set(p.getMateId(), p.getReadId(), newThisOffset + p.getOffset());
                }
                for (ReadHeadInfo p : other.unflippedReadIds) {
                    unflippedReadIds.add(p);
                }
                for (ReadHeadInfo p : other.flippedReadIds) {
                    flippedReadIds.add(p);
                }
                break;
        }
    }

    /**
     * Debug helper function to find the edge associated with the given kmer, checking all directions. If the edge doesn't exist in any direction, returns null
     */
    public NeighborInfo findEdge(final VKmer kmer) {
        for (EDGETYPE et : EDGETYPE.values()) {
            if (edges[et.get()].containsKey(kmer)) {
                return new NeighborInfo(et, kmer, edges[et.get()].get(kmer));
            }
        }
        return null;
    }

    public int degree(DIR direction) {
        int totalDegree = 0;
        for (EDGETYPE et : DIR.edgeTypesInDir(direction)) {
            totalDegree += edges[et.get()].size();
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
        return unflippedReadIds.size() > 0 || flippedReadIds.size() > 0;
    }

}
