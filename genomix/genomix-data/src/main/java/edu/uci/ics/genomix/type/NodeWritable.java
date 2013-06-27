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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

public class NodeWritable implements WritableComparable<NodeWritable>, Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    public static final NodeWritable EMPTY_NODE = new NodeWritable(0);

    // merge/update directions
    public static class DirectionFlag {
        public static final byte DIR_FF = 0b00 << 0;
        public static final byte DIR_FR = 0b01 << 0;
        public static final byte DIR_RF = 0b10 << 0;
        public static final byte DIR_RR = 0b11 << 0;
        public static final byte DIR_MASK = 0b11 << 0;
    }

    private PositionWritable nodeID;
    private PositionListWritable forwardForwardList;
    private PositionListWritable forwardReverseList;
    private PositionListWritable reverseForwardList;
    private PositionListWritable reverseReverseList;
    private KmerBytesWritable kmer;

    public NodeWritable() {
        this(21);
    }

    public NodeWritable(int kmerSize) {
        nodeID = new PositionWritable(0, (byte) 0);
        forwardForwardList = new PositionListWritable();
        forwardReverseList = new PositionListWritable();
        reverseForwardList = new PositionListWritable();
        reverseReverseList = new PositionListWritable();
        kmer = new KmerBytesWritable(kmerSize);
    }

    public NodeWritable(PositionWritable nodeID, PositionListWritable FFList, PositionListWritable FRList,
            PositionListWritable RFList, PositionListWritable RRList, KmerBytesWritable kmer) {
        this(kmer.getKmerLength());
        this.nodeID.set(nodeID);
        forwardForwardList.set(FFList);
        forwardReverseList.set(FRList);
        reverseForwardList.set(RFList);
        reverseReverseList.set(RRList);
        kmer.set(kmer);
    }

    public void set(PositionWritable nodeID, PositionListWritable FFList, PositionListWritable FRList,
            PositionListWritable RFList, PositionListWritable RRList, KmerBytesWritable kmer) {
        this.nodeID.set(nodeID);
        this.forwardForwardList.set(FFList);
        this.forwardReverseList.set(FRList);
        this.reverseForwardList.set(RFList);
        this.reverseReverseList.set(RRList);
        this.kmer.set(kmer);
    }

    public void setNodeID(PositionWritable ref) {
        this.setNodeID(ref.getReadID(), ref.getPosInRead());
    }

    public void setNodeID(int readID, byte posInRead) {
        nodeID.set(readID, posInRead);
    }

    public void setKmer(KmerBytesWritable right) {
        this.kmer.set(right);
    }

    public void reset(int kmerSize) {
        nodeID.set(0, (byte) 0);
        forwardForwardList.reset();
        forwardReverseList.reset();
        reverseForwardList.reset();
        reverseReverseList.reset();
        kmer.reset(kmerSize);
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

    public PositionWritable getNodeID() {
        return nodeID;
    }

    public KmerBytesWritable getKmer() {
        return kmer;
    }

    public int getCount() {
        return kmer.getKmerLength();
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

    public void set(NodeWritable node) {
        this.nodeID.set(node.getNodeID().getReadID(), node.getNodeID().getPosInRead());
        this.forwardForwardList.set(node.forwardForwardList);
        this.forwardReverseList.set(node.forwardReverseList);
        this.reverseForwardList.set(node.reverseForwardList);
        this.reverseReverseList.set(node.reverseReverseList);
        this.kmer.set(node.kmer);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.nodeID.readFields(in);
        this.forwardForwardList.readFields(in);
        this.forwardReverseList.readFields(in);
        this.reverseForwardList.readFields(in);
        this.reverseReverseList.readFields(in);
        this.kmer.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.nodeID.write(out);
        this.forwardForwardList.write(out);
        this.forwardReverseList.write(out);
        this.reverseForwardList.write(out);
        this.reverseReverseList.write(out);
        this.kmer.write(out);
    }

    @Override
    public int compareTo(NodeWritable other) {
        return this.nodeID.compareTo(other.nodeID);
    }

    @Override
    public int hashCode() {
        return nodeID.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NodeWritable) {
            NodeWritable nw = (NodeWritable) o;
            return (this.nodeID.equals(nw.nodeID) && this.forwardForwardList.equals(nw.forwardForwardList)
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
        sbuilder.append(nodeID.toString()).append('\t');
        sbuilder.append(forwardForwardList.toString()).append('\t');
        sbuilder.append(forwardReverseList.toString()).append('\t');
        sbuilder.append(reverseForwardList.toString()).append('\t');
        sbuilder.append(reverseReverseList.toString()).append('\t');
        sbuilder.append(kmer.toString()).append(')');
        return sbuilder.toString();
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

}
