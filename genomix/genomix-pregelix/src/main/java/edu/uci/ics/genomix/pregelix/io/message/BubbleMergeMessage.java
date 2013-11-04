package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;

public class BubbleMergeMessage extends MessageWritable {

    class BUBBLEMERGE_MESSAGE_FIELDS extends MESSAGE_FIELDS {
        public static final byte NODE = 1 << 1; // used in BubbleMergeMessage
        public static final byte MAJOR_VERTEX_ID_AND_MAJOR_TO_BUBBLE_EDGETYPE = 1 << 2; // used in BubbleMergeMessage
        public static final byte MINOR_VERTEX_ID_AND_MINOR_TO_BUBBLE_EDGETYPE = 1 << 3; // used in BubbleMergeMessage
        public static final byte TOP_COVERAGE_VERTEX_ID = 1 << 4; // used in BubbleMergeMessage
    }

    private VKmer majorVertexId; //use for MergeBubble
    private VKmer minorVertexId;
    private Node node; //except kmer, other field should be updated when MergeBubble
    private EDGETYPE majorToBubbleEdgetype;
    private EDGETYPE minorToBubbleEdgetype;
    private VKmer topCoverageVertexId;

    public BubbleMergeMessage() {
        super();
        majorVertexId = new VKmer();
        minorVertexId = new VKmer();
        node = new Node();
        majorToBubbleEdgetype = EDGETYPE.FF;
        minorToBubbleEdgetype = EDGETYPE.FF;
        topCoverageVertexId = new VKmer();
    }

    public BubbleMergeMessage(BubbleMergeMessage msg) {
        set(msg);
    }

    public void set(BubbleMergeMessage msg) {
        this.setSourceVertexId(msg.getSourceVertexId());
        this.setFlag(msg.getFlag());
        this.setMajorVertexId(msg.getMajorVertexId());
        this.setMinorVertexId(msg.getMinorVertexId());
        this.setNode(msg.node);
        this.setMajorToBubbleEdgetype(msg.getMajorToBubbleEdgetype());
        this.setMinorToBubbleEdgetype(msg.getMinorToBubbleEdgetype());
        this.setTopCoverageVertexId(msg.topCoverageVertexId);
    }

    public void reset() {
        super.reset();
        majorVertexId.reset(0);
        minorVertexId.reset(0);
        node.reset();
        majorToBubbleEdgetype = EDGETYPE.FF;
        minorToBubbleEdgetype = EDGETYPE.FF;
        topCoverageVertexId.reset(0);
    }

    public EdgeMap getMinorToBubbleEdgeMap() {
        return node.getEdgeMap(getMinorToBubbleEdgetype().mirror());
    }

    public void addNewMajorToBubbleEdges(boolean sameOrientation, BubbleMergeMessage msg, VKmer topKmer) {
        validMessageFlag |= BUBBLEMERGE_MESSAGE_FIELDS.MAJOR_VERTEX_ID_AND_MAJOR_TO_BUBBLE_EDGETYPE;
        EDGETYPE majorToBubble = msg.getMajorToBubbleEdgetype();
        ReadIdSet newReadIds = msg.getNode().getEdgeMap(majorToBubble.mirror())
                .get(msg.getMajorVertexId());
        node.getEdgeMap(sameOrientation ? majorToBubble : majorToBubble.flipNeighbor()).unionAdd(topKmer, newReadIds);
    }

    public VKmer getMajorVertexId() {
        return majorVertexId;
    }

    public void setMajorVertexId(VKmer majorVertexId) {
        validMessageFlag |= BUBBLEMERGE_MESSAGE_FIELDS.MAJOR_VERTEX_ID_AND_MAJOR_TO_BUBBLE_EDGETYPE;
        if (this.majorVertexId == null)
            this.majorVertexId = new VKmer();
        this.majorVertexId.setAsCopy(majorVertexId);
    }

    public VKmer getMinorVertexId() {
        return minorVertexId;
    }

    public void setMinorVertexId(VKmer minorVertexId) {
        validMessageFlag |= BUBBLEMERGE_MESSAGE_FIELDS.MINOR_VERTEX_ID_AND_MINOR_TO_BUBBLE_EDGETYPE;
        if (this.minorVertexId == null)
            this.minorVertexId = new VKmer();
        this.minorVertexId.setAsCopy(minorVertexId);
    }

    public VKmer getTopCoverageVertexId() {
        return topCoverageVertexId;
    }

    public void setTopCoverageVertexId(VKmer topCoverageVertexId) {
        validMessageFlag |= BUBBLEMERGE_MESSAGE_FIELDS.TOP_COVERAGE_VERTEX_ID;
        if (this.topCoverageVertexId == null)
            this.topCoverageVertexId = new VKmer();
        this.topCoverageVertexId.setAsCopy(topCoverageVertexId);
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        if (this.node == null)
            this.node = new Node();
        this.node.setAsCopy(node);
    }

    public EDGETYPE getMajorToBubbleEdgetype() {
        return majorToBubbleEdgetype;
    }

    public void setMajorToBubbleEdgetype(EDGETYPE majorToBubbleEdgetype) {
        validMessageFlag |= BUBBLEMERGE_MESSAGE_FIELDS.MAJOR_VERTEX_ID_AND_MAJOR_TO_BUBBLE_EDGETYPE;
        this.majorToBubbleEdgetype = majorToBubbleEdgetype;
    }

    public EDGETYPE getMinorToBubbleEdgetype() {
        return minorToBubbleEdgetype;
    }

    public void setMinorToBubbleEdgetype(EDGETYPE minorToBubbleEdgetype) {
        validMessageFlag |= BUBBLEMERGE_MESSAGE_FIELDS.MINOR_VERTEX_ID_AND_MINOR_TO_BUBBLE_EDGETYPE;
        this.minorToBubbleEdgetype = minorToBubbleEdgetype;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        if ((validMessageFlag & BUBBLEMERGE_MESSAGE_FIELDS.MAJOR_VERTEX_ID_AND_MAJOR_TO_BUBBLE_EDGETYPE) > 0) {
            majorVertexId.readFields(in);
            majorToBubbleEdgetype = EDGETYPE.fromByte(in.readByte());
        }
        if ((validMessageFlag & BUBBLEMERGE_MESSAGE_FIELDS.MINOR_VERTEX_ID_AND_MINOR_TO_BUBBLE_EDGETYPE) > 0) {
            minorVertexId.readFields(in);
            minorToBubbleEdgetype = EDGETYPE.fromByte(in.readByte());
        }
        if ((validMessageFlag & BUBBLEMERGE_MESSAGE_FIELDS.NODE) > 0)
            node.readFields(in);
        if ((validMessageFlag & BUBBLEMERGE_MESSAGE_FIELDS.TOP_COVERAGE_VERTEX_ID) > 0)
            topCoverageVertexId.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if ((validMessageFlag & BUBBLEMERGE_MESSAGE_FIELDS.MAJOR_VERTEX_ID_AND_MAJOR_TO_BUBBLE_EDGETYPE) > 0) {
            majorVertexId.write(out);
            out.writeByte(majorToBubbleEdgetype.get());
        }
        if ((validMessageFlag & BUBBLEMERGE_MESSAGE_FIELDS.MINOR_VERTEX_ID_AND_MINOR_TO_BUBBLE_EDGETYPE) > 0) {
            minorVertexId.write(out);
            out.writeByte(minorToBubbleEdgetype.get());
        }
        if ((validMessageFlag & BUBBLEMERGE_MESSAGE_FIELDS.NODE) > 0)
            node.write(out);
        if ((validMessageFlag & BUBBLEMERGE_MESSAGE_FIELDS.TOP_COVERAGE_VERTEX_ID) > 0)
            topCoverageVertexId.write(out);
    }

    public static class SortByCoverage implements Comparator<BubbleMergeMessage> {
        @Override
        public int compare(BubbleMergeMessage left, BubbleMergeMessage right) {
            return -Float.compare(left.node.getAverageCoverage(), right.node.getAverageCoverage());
        }
    }

    public boolean sameOrientation(BubbleMergeMessage other) {
        return EDGETYPE.sameOrientation(this.majorToBubbleEdgetype, other.majorToBubbleEdgetype);
    }

    public float computeDissimilar(BubbleMergeMessage other) {
        boolean sameOrientation = sameOrientation(other);
        return this.getNode().getInternalKmer().fracDissimilar(sameOrientation, other.getNode().getInternalKmer());
    }
}
