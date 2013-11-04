package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;

public class PathMergeMessage extends MessageWritable {

    class PATHMERGE_MESSAGE_FIELDS extends MESSAGE_FIELDS {
        public static final byte NODE = 1 << 1; // used in subclass: PathMergeMessage
    }

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

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.validMessageFlag |= PATHMERGE_MESSAGE_FIELDS.NODE;
        this.node.setAsCopy(node);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        if ((validMessageFlag & PATHMERGE_MESSAGE_FIELDS.NODE) > 0)
            node.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if ((validMessageFlag & PATHMERGE_MESSAGE_FIELDS.NODE) > 0)
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
