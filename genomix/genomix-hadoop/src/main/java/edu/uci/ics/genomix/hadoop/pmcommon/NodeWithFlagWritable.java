package edu.uci.ics.genomix.hadoop.pmcommon;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import javax.management.RuntimeErrorException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;

/*
 * Simple "Message" class, allowing a NodeWritable to be sent, along with a message flag.
 * This class is used as the value in several MapReduce algorithms.
 */
public class NodeWithFlagWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {
    private byte flag;
    private NodeWritable node;
    
    public static class MessageFlag {
        public static final byte EMPTY_MESSAGE = 0;
        // message types
        public static final byte MSG_SELF = 0b01 << 0;
        public static final byte MSG_UPDATE_MERGE = 0b10 << 0;
        public static final byte MSG_UPDATE_EDGE = 0b11 << 0;
        public static final byte MSG_MASK = 0b11 << 0; 
        // merge/update directions
        public static final byte DIR_FF = 0b00 << 2;
        public static final byte DIR_FR = 0b01 << 2;
        public static final byte DIR_RF = 0b10 << 2;
        public static final byte DIR_RR = 0b11 << 2;
        public static final byte DIR_MASK = 0b11 << 2;
        // additional info
        public static final byte IS_HEAD = 0b1 << 4;
        public static final byte IS_TAIL = 0b1 << 5;
        // extra bit used differently in each operation
        public static final byte EXTRA_FLAG = 1 << 6;
    }
    
    /*
     * Process any changes to @node contained in @updateMsg.  This includes merges and edge updates
     */
    public static void processUpdates(NodeWritable node, NodeWithFlagWritable updateMsg, int kmerSize) throws IOException {
        byte updateFlag = updateMsg.getFlag();
        NodeWritable updateNode = updateMsg.getNode();
        if ((updateFlag & MessageFlag.MSG_UPDATE_EDGE) == MessageFlag.MSG_UPDATE_EDGE) {
            // this message wants to update the edges of node.
            // remove position and merge its position lists with node
            if (!updateNode.equals(NodeWritable.EMPTY_NODE)) {
                // need to remove updateNode from the specified PositionList
                switch(updateFlag & MessageFlag.DIR_MASK) {
                    case MessageFlag.DIR_FF:
                        node.getFFList().remove(updateNode.getNodeID());
                        break;
                    case MessageFlag.DIR_FR:
                        node.getFRList().remove(updateNode.getNodeID());
                        break;
                    case MessageFlag.DIR_RF:
                        node.getRFList().remove(updateNode.getNodeID());
                        break;
                    case MessageFlag.DIR_RR:
                        node.getRRList().remove(updateNode.getNodeID());
                        break;
                    default:
                        throw new IOException("Unrecognized direction in updateFlag: " + updateFlag);
                }
            }
            // now merge positionlists from update and node
            node.getFFList().appendList(updateNode.getFFList());
            node.getFRList().appendList(updateNode.getFRList());
            node.getRFList().appendList(updateNode.getRFList());
            node.getRRList().appendList(updateNode.getRRList());
        } else if ((updateFlag & MessageFlag.MSG_UPDATE_MERGE) == MessageFlag.MSG_UPDATE_MERGE) {
            // this message wants to merge node with updateNode.
            // the direction flag indicates how the merge should take place.
            switch(updateFlag & MessageFlag.DIR_MASK) {
                case MessageFlag.DIR_FF:
                    node.getKmer().mergeWithFFKmer(kmerSize, updateNode.getKmer());
                    node.getFFList().set(updateNode.getFFList());
                    break;
                case MessageFlag.DIR_FR:
                    // FIXME not sure if this should be reverse-complement or just reverse...
                    node.getKmer().mergeWithFFKmer(kmerSize, updateNode.getKmer());
                    node.getFRList().set(updateNode.getFRList());
                    break;
                case MessageFlag.DIR_RF:
                    
                    break;
                case MessageFlag.DIR_RR:
                    node.getKmer().mergeWithRRKmer(kmerSize, updateNode.getKmer());
                    node.getRRList().set(updateNode.getRRList());
                    break;
                default:
                    throw new IOException("Unrecognized direction in updateFlag: " + updateFlag);
            }
        }
    }

    public NodeWithFlagWritable() {
        this(0);
    }

    public NodeWithFlagWritable(int k) {
        this.flag = 0;
        this.node = new NodeWritable(k);
    }

    public NodeWithFlagWritable(byte flag, int kmerSize) {
        this.flag = flag;
        this.node = new NodeWritable(kmerSize);
    }
    
    public NodeWithFlagWritable(byte flag, NodeWritable node) {
        this(node.getKmer().getKmerLength());
        set(flag, node);
    }

    public void set(NodeWithFlagWritable right) {
        set(right.getFlag(), right.getNode());
    }

    public void set(byte flag, NodeWritable node) {
        this.node.set(node);
        this.flag = flag;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        node.readFields(arg0);
        flag = arg0.readByte();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        node.write(arg0);
        arg0.writeByte(flag);
    }

    public NodeWritable getNode() {
        if (node.getCount() != 0) {
            return node;
        }
        return null;
    }

    public byte getFlag() {
        return this.flag;
    }

    public String toString() {
        return node.toString() + '\t' + String.valueOf(flag);
    }

    @Override
    public byte[] getBytes() {
        if (node.getCount() != 0) {
            return node.getKmer().getBytes();
        } else
            return null;
    }

    @Override
    public int getLength() {
        return node.getCount();
    }

    @Override
    public int hashCode() {
//        return super.hashCode() + flag + node.hashCode();
        return flag + node.hashCode();
    }

    @Override
    public boolean equals(Object rightObj) {
        if (rightObj instanceof NodeWithFlagWritable) {
            NodeWithFlagWritable rightMessage = (NodeWithFlagWritable) rightObj;
            return (this.flag == rightMessage.flag && this.node.equals(rightMessage.node));
        }
        return false;
    }
}