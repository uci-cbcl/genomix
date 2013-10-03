package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.message.P2PathMergeMessage.P2MessageType;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;

public class P2VertexValue extends VertexValueWritable {

    private static final long serialVersionUID = -6600330062969997327L;

    private Node prependMergeNode;
    private Node appendMergeNode;

    private HashMapWritable<VKmer, KmerAndDir> apexMap; //<apexId, deleteKmerAndDir>

    public P2VertexValue() {
        super();
        prependMergeNode = new Node();
        appendMergeNode = new Node();
        apexMap = new HashMapWritable<VKmer, KmerAndDir>();
    }

    public VertexValueWritable get() {
        VertexValueWritable tmpValue = new VertexValueWritable();
        tmpValue.setAsCopy(getNode());
        tmpValue.setState(getState());
        tmpValue.setFakeVertex(isFakeVertex());
        tmpValue.setCounters(getCounters());
        tmpValue.setScaffoldingMap(getScaffoldingMap());

        return tmpValue;
    }

    public void reset() {
        super.reset();
        prependMergeNode.reset();
        appendMergeNode.reset();
        //        apexMap.clear();
    }

    public Node getMergeNode(byte mergeMsgType) {
        switch (mergeMsgType) {
            case P2MessageType.FROM_PREDECESSOR:
                return getPrependMergeNode();
            case P2MessageType.FROM_SUCCESSOR:
                return getAppendMergeNode();
        }
        return null;
    }

    /**
     * process finalNode
     */
    public void processFinalNode() {
        int internalKmerLength = getNode().getInternalKmer().getKmerLetterLength();
        setNode(prependMergeNode);
        getNode().mergeWithNodeWithoutKmer(appendMergeNode);

        String prepend = prependMergeNode.getInternalKmer().toString();
        String append = appendMergeNode.getInternalKmer().toString();
        String merge = prepend + append.substring(internalKmerLength);
        getNode().getInternalKmer().setFromStringBytes(merge.length(), merge.getBytes(), 0);
    }

    public Node getPrependMergeNode() {
        return prependMergeNode;
    }

    public void setPrependMergeNode(Node prependMergeNode) {
        this.prependMergeNode.setAsCopy(prependMergeNode);
    }

    public Node getAppendMergeNode() {
        return appendMergeNode;
    }

    public void setAppendMergeNode(Node appendMergeNode) {
        this.appendMergeNode.setAsCopy(appendMergeNode);
    }

    public HashMapWritable<VKmer, KmerAndDir> getApexMap() {
        return apexMap;
    }

    public void setApexMap(HashMapWritable<VKmer, KmerAndDir> apexMap) {
        this.apexMap = new HashMapWritable<VKmer, KmerAndDir>(apexMap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        prependMergeNode.readFields(in);
        appendMergeNode.readFields(in);
        apexMap.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        prependMergeNode.write(out);
        appendMergeNode.write(out);
        apexMap.write(out);
    }
}
