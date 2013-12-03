package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmerList;

public class TipRemoveWithSearchMessage extends MessageWritable {
    private Integer visitedLength = null;
    private VKmerList visitedNodes = null;

    /**
     * Include the given node in incomingMsg's path of vertices
     * 
     * @param v
     *            the vertex to add to incomingMsg's path
     * @param incomingMsg
     */
    public void visitNode(Node n) {
        if (visitedLength == null) {
            setVisitedLength(0);
        }
        visitedLength += n.getKmerLength() - Kmer.getKmerLength() + 1;
        getVisitedNodes().append(n.getInternalKmer());
    }

    public int getVisitedLength() {
        return visitedLength;
    }

    public void setVisitedLength(int visitedLength) {
        this.visitedLength = visitedLength;
    }

    public VKmerList getVisitedNodes() {
        if (visitedNodes == null) {
            visitedNodes = new VKmerList();
        }
        return visitedNodes;
    }

    public void setVisitedNodes(VKmerList visitedNodes) {
        if (visitedNodes == null || visitedNodes.size() == 0) {
            this.visitedNodes = null;
        } else {
            getVisitedNodes().setAsCopy(visitedNodes);
        }
    }

    @Override
    public void reset() {
        super.reset();
        visitedLength = null;
        visitedNodes = null;
    }

    protected static class TIP_REMOVE_FIELDS extends MESSAGE_FIELDS {
        public static final byte VISITED_LENGTH = 1 << 1;
        public static final byte VISITED_NODES = 1 << 2;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if ((messageFields & TIP_REMOVE_FIELDS.VISITED_LENGTH) != 0) {
            setVisitedLength(in.readInt());
        }
        if ((messageFields & TIP_REMOVE_FIELDS.VISITED_NODES) != 0) {
            getVisitedNodes().readFields(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if (visitedLength != null) {
            out.writeInt(visitedLength);
        }
        if (visitedNodes != null && visitedNodes.size() != 0) {
            visitedNodes.write(out);
        }
    }

    @Override
    protected byte getActiveMessageFields() {
        byte messageFields = super.getActiveMessageFields();
        if (visitedLength != null) {
            messageFields |= TIP_REMOVE_FIELDS.VISITED_LENGTH;
        }
        if (visitedNodes != null && visitedNodes.size() != 0) {
            messageFields |= TIP_REMOVE_FIELDS.VISITED_NODES;
        }
        return messageFields;
    }
}
