package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;

public class PathMergeMessage extends MessageWritable {

    protected static class PATHMERGE_MESSAGE_FIELDS extends MESSAGE_FIELDS {
        public static final byte NODE = 1 << 1; // used in subclass: PathMergeMessage
    }

    private Node node;

    public PathMergeMessage() {
        super();
        node = null;
    }

    public PathMergeMessage(PathMergeMessage other) {
        this();
        this.setAsCopy(other);
    }

    public void setAsCopy(PathMergeMessage other) {
        super.setAsCopy(other);
        getNode().setAsCopy(other.getNode());
    }

    @Override
    public void reset() {
        super.reset();
        node = null;
    }

    public VKmer getInternalKmer() {
        return getNode().getInternalKmer();
    }

    public void setInternalKmer(VKmer internalKmer) {
        getNode().setInternalKmer(internalKmer);
    }

    public EdgeMap getEdgeMap(EDGETYPE edgeType) {
        return getNode().getEdgeMap(edgeType);
    }

    public Node getNode() {
        if (node == null) {
            node = new Node();
        }
        return node;
    }

    public void setNode(Node node) {
        getNode().setAsCopy(node);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if ((messageFields & PATHMERGE_MESSAGE_FIELDS.NODE) != 0) {
            getNode().readFields(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if (node != null) {
            node.write(out);
        }
    }

    @Override
    protected byte getActiveMessageFields() {
        byte messageFields = super.getActiveMessageFields();
        if (node != null) {
            messageFields |= PATHMERGE_MESSAGE_FIELDS.NODE;
        }
        return messageFields;
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        sbuilder.append("src:[");
        sbuilder.append(sourceVertexId == null ? "null" : getSourceVertexId().toString()).append(']').append("\t");
        sbuilder.append("node:");
        sbuilder.append(node == null ? "null" : node.toString()).append("\t");
        sbuilder.append('}');
        return sbuilder.toString();
    }
}
