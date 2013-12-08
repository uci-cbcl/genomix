package edu.uci.ics.genomix.pregelix.operator.simplebubblemerge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;

public class SimpleBubbleMergeMessage extends MessageWritable {

    protected class BUBBLEMERGE_MESSAGE_FIELDS extends MESSAGE_FIELDS {
        public static final byte NODE = 1 << 1; // used in BubbleMergeMessage
        public static final byte MAJOR_VERTEX_ID = 1 << 2; // used in BubbleMergeMessage
        public static final byte MINOR_VERTEX_ID = 1 << 3; // used in BubbleMergeMessage
        public static final byte MAJOR_TO_BUBBLE_EDGETYPE = 1 << 4; // used in BubbleMergeMessage
        public static final byte MINOR_TO_BUBBLE_EDGETYPE = 1 << 5; // used in BubbleMergeMessage
        public static final byte TOP_COVERAGE_VERTEX_ID = 1 << 6; // used in BubbleMergeMessage
    }

    private VKmer majorVertexId; //use for MergeBubble
    private VKmer minorVertexId;
    private Node node; //except kmer, other field should be updated when MergeBubble
    private EDGETYPE majorToBubbleEdgetype;
    private EDGETYPE minorToBubbleEdgetype;
    private VKmer topCoverageVertexId;

    public SimpleBubbleMergeMessage() {
        super();
        majorVertexId = null;
        minorVertexId = null;
        node = null;
        majorToBubbleEdgetype = null;
        minorToBubbleEdgetype = null;
        topCoverageVertexId = null;
    }

    public SimpleBubbleMergeMessage(SimpleBubbleMergeMessage msg) {
        set(msg);
    }

    public void set(SimpleBubbleMergeMessage msg) {
        this.setSourceVertexId(msg.sourceVertexId);
        this.setFlag(msg.flag);
        this.setMajorVertexId(msg.majorVertexId);
        this.setMinorVertexId(msg.minorVertexId);
        this.setNode(msg.node);
        this.setMajorToBubbleEdgetype(msg.majorToBubbleEdgetype);
        this.setMinorToBubbleEdgetype(msg.minorToBubbleEdgetype);
        this.setTopCoverageVertexId(msg.topCoverageVertexId);
    }

    @Override
    public void reset() {
        super.reset();
        majorVertexId = null;
        minorVertexId = null;
        node = null;
        majorToBubbleEdgetype = null;
        minorToBubbleEdgetype = null;
        topCoverageVertexId = null;
    }

    public VKmerList getMinorToBubbleEdges() {
        if (node == null) {
            node = new Node();
        }
        return node.getEdges(getMinorToBubbleEdgetype().mirror());
    }

    public VKmer getMajorVertexId() {
        if (majorVertexId == null) {
            majorVertexId = new VKmer();
        }
        return majorVertexId;
    }

    public void setMajorVertexId(VKmer majorVertexId) {
        getMajorVertexId().setAsCopy(majorVertexId);
    }

    public VKmer getMinorVertexId() {
        if (minorVertexId == null) {
            minorVertexId = new VKmer();
        }
        return minorVertexId;
    }

    public void setMinorVertexId(VKmer minorVertexId) {
        getMinorVertexId().setAsCopy(minorVertexId);
    }

    public VKmer getTopCoverageVertexId() {
        if (topCoverageVertexId == null) {
            topCoverageVertexId = new VKmer();
        }
        return topCoverageVertexId;
    }

    public void setTopCoverageVertexId(VKmer topCoverageVertexId) {
        getTopCoverageVertexId().setAsCopy(topCoverageVertexId);
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

    public EDGETYPE getMajorToBubbleEdgetype() {
        return majorToBubbleEdgetype;
    }

    public void setMajorToBubbleEdgetype(EDGETYPE majorToBubbleEdgetype) {
        this.majorToBubbleEdgetype = majorToBubbleEdgetype;
    }

    public EDGETYPE getMinorToBubbleEdgetype() {
        return minorToBubbleEdgetype;
    }

    public void setMinorToBubbleEdgetype(EDGETYPE minorToBubbleEdgetype) {
        this.minorToBubbleEdgetype = minorToBubbleEdgetype;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if ((messageFields & BUBBLEMERGE_MESSAGE_FIELDS.MAJOR_VERTEX_ID) != 0) {
            getMajorVertexId().readFields(in);
        }
        if ((messageFields & BUBBLEMERGE_MESSAGE_FIELDS.MAJOR_TO_BUBBLE_EDGETYPE) != 0) {
            majorToBubbleEdgetype = EDGETYPE.fromByte(in.readByte());
        }
        if ((messageFields & BUBBLEMERGE_MESSAGE_FIELDS.MINOR_VERTEX_ID) != 0) {
            getMinorVertexId().readFields(in);
        }
        if ((messageFields & BUBBLEMERGE_MESSAGE_FIELDS.MINOR_TO_BUBBLE_EDGETYPE) != 0) {
            minorToBubbleEdgetype = EDGETYPE.fromByte(in.readByte());
        }
        if ((messageFields & BUBBLEMERGE_MESSAGE_FIELDS.NODE) != 0) {
            getNode().readFields(in);
        }
        if ((messageFields & BUBBLEMERGE_MESSAGE_FIELDS.TOP_COVERAGE_VERTEX_ID) != 0) {
            getTopCoverageVertexId().readFields(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if (majorVertexId != null) {
            majorVertexId.write(out);
        }
        if (majorToBubbleEdgetype != null) {
            out.writeByte(majorToBubbleEdgetype.get());
        }
        if (minorVertexId != null) {
            minorVertexId.write(out);
        }
        if (minorToBubbleEdgetype != null) {
            out.writeByte(minorToBubbleEdgetype.get());
        }
        if (node != null) {
            node.write(out);
        }
        if (topCoverageVertexId != null) {
            topCoverageVertexId.write(out);
        }
    }

    @Override
    protected byte getActiveMessageFields() {
        byte messageFields = super.getActiveMessageFields();
        if (majorVertexId != null) {
            messageFields |= BUBBLEMERGE_MESSAGE_FIELDS.MAJOR_VERTEX_ID;
        }
        if (minorVertexId != null) {
            messageFields |= BUBBLEMERGE_MESSAGE_FIELDS.MINOR_VERTEX_ID;
        }
        if (majorToBubbleEdgetype != null) {
            messageFields |= BUBBLEMERGE_MESSAGE_FIELDS.MAJOR_TO_BUBBLE_EDGETYPE;
        }
        if (minorToBubbleEdgetype != null) {
            messageFields |= BUBBLEMERGE_MESSAGE_FIELDS.MINOR_TO_BUBBLE_EDGETYPE;
        }
        if (node != null) {
            messageFields |= BUBBLEMERGE_MESSAGE_FIELDS.NODE;
        }
        if (topCoverageVertexId != null) {
            messageFields |= BUBBLEMERGE_MESSAGE_FIELDS.TOP_COVERAGE_VERTEX_ID;
        }
        return messageFields;
    }

    public static class SortByCoverage implements Comparator<SimpleBubbleMergeMessage> {
        @Override
        public int compare(SimpleBubbleMergeMessage left, SimpleBubbleMergeMessage right) {
            return -Float.compare(left.node.getAverageCoverage(), right.node.getAverageCoverage());
        }
    }

    public boolean sameOrientation(SimpleBubbleMergeMessage other) {
        return EDGETYPE.sameOrientation(this.majorToBubbleEdgetype, other.majorToBubbleEdgetype);
    }

    public float computeDissimilar(SimpleBubbleMergeMessage other) {
        boolean sameOrientation = sameOrientation(other);
        return this.getNode().getInternalKmer().fracDissimilar(sameOrientation, other.getNode().getInternalKmer());
    }
    
    public float editDistance(SimpleBubbleMergeMessage other) {
        boolean sameOrientation = sameOrientation(other);
        return this.getNode().getInternalKmer().editDistance(sameOrientation, other.getNode().getInternalKmer());
    }
}
