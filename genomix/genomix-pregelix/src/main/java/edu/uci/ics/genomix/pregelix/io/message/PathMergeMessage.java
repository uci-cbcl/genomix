package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.Node.EDGETYPE;
import edu.uci.ics.genomix.type.ReadHeadSet;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;

public class PathMergeMessage extends MessageWritable {

    private Node node;

    public PathMergeMessage() {
        super();
        node = new Node();
    }

    public PathMergeMessage(PathMergeMessage other) {
        this();
        this.setAsCopy(other);
    }

    public void setAsCopy(PathMergeMessage other) {
        super.setAsCopy(other);
        this.node.setAsCopy(other.getNode());
    }

    public void reset() {
        super.reset();
        node.reset();
    }

    public VKmer getInternalKmer() {
        return node.getInternalKmer();
    }

    public void setInternalKmer(VKmer internalKmer) {
        this.node.setInternalKmer(internalKmer);
    }

    public EdgeMap getEdgeList(EDGETYPE edgeType) {
        return node.getEdgeMap(edgeType);
    }

    public Entry<VKmer, ReadIdSet> getNeighborEdge() {
        for (EDGETYPE e : EDGETYPE.values()) {
            if (!getEdgeList(e).isEmpty()) {
                return getEdgeList(e).firstEntry();
            }
        }
        return null;
    }

    public void setEdgeList(EDGETYPE edgeType, EdgeMap edgeList) {
        this.node.setEdgeMap(edgeType, edgeList);
    }

    public ReadHeadSet getStartReads() {
        return this.node.getStartReads();
    }

    public void setStartReads(ReadHeadSet startReads) {
        this.node.setStartReads(startReads);
    }

    public ReadHeadSet getEndReads() {
        return this.node.getEndReads();
    }

    public void setEndReads(ReadHeadSet endReads) {
        this.node.setEndReads(endReads);
    }

    public void setAverageCoverage(float coverage) {
        this.node.setAverageCoverage(coverage);
    }

    public float getAvgCoverage() {
        return this.node.getAverageCoverage();
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node.setAsCopy(node);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        node.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        node.write(out);
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        sbuilder.append("src:[");
        sbuilder.append(getSourceVertexId().toString()).append(']').append("\t");
        sbuilder.append("node:");
        sbuilder.append(node.toString()).append("\t");
        sbuilder.append('}');
        return sbuilder.toString();
    }
}
