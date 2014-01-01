package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;

public class RayMessage extends MessageWritable {

    private RayMessageType messageType;

    public enum RayMessageType {
        REQUEST_KMER,
        REQUEST_SCORE,
        AGGREGATE_SCORE,
        PRUNE_EDGE,
        CONTINUE_WALK,
        STOP;
        public static final RayMessageType[] values = values();
    }

    /** for REQUEST_KMER, REQUEST_SCORE, AGGREGATE_SCORE, CONTINUE_WALK */
    private VKmerList walkIds = null; // the kmer id's of the previous frontiers (now part of this walk)
    private ArrayList<Integer> walkOffsets = null; // for each walk node, its start offset in the complete walk
    private Integer walkLength = null; // total number of bases in this walk including the frontier; the offset for current candidates
    private VKmer accumulatedWalkKmer = null; // the kmer for this complete walk 

    /** for REQUEST_KMER, PRUNE_EDGE, CONTINUE_WALK */
    private EDGETYPE candidateToFrontierEdgeType = null; // the edge type from the candidate's perspective back to the frontier node

    /** for REQUEST_KMER, CONTINUE_WALK */
    private Boolean frontierFlipped = false; // wrt the initial search direction

    /** for REQUEST_SCORE */
    private VKmer toScoreId = null; // id of candidate to be scored by the walk nodes
    private VKmer toScoreKmer = null; // internalKmer of candidate

    /** for AGGREGATE_SCORE */
    private RayScores singleEndScores;
    private RayScores pairedEndScores;

    public RayMessage() {

    }

    public RayMessage(RayMessage other) {
        setAsCopy(other);
    }

    /**
     * add the given vertex to the end of this walk
     * 
     * @param id
     * @param vertex
     * @param accumulatedKmerDir
     */
    public void visitNode(VKmer id, RayValue vertex, DIR accumulatedKmerDir) {
        getWalkIds().append(id);
        getWalkOffsets().add(getWalkLength());
        setWalkLength(getWalkLength() + vertex.getKmerLength() - Kmer.getKmerLength() + 1);

        if (accumulatedWalkKmer == null || accumulatedWalkKmer.getKmerLetterLength() == 0) {
            // kmer oriented st always merge in an F dir (seed's offset always 0)
            setAccumulatedWalkKmer(accumulatedKmerDir == DIR.FORWARD ? vertex.getInternalKmer() : vertex
                    .getInternalKmer().reverse());
        } else {
            EDGETYPE accumulatedToVertexET = !vertex.flippedFromInitialDirection ? EDGETYPE.FF : EDGETYPE.FR;
            getAccumulatedWalkKmer().mergeWithKmerInDir(accumulatedToVertexET, Kmer.getKmerLength(),
                    vertex.getInternalKmer());
        }
    }

    /**
     * @return whether the candidate node represented by this message is a flipped or unflipped node
     */
    public boolean isCandidateFlipped() {
        // if the frontier was flipped and I came across a FR/RF, I'm back to unflipped
        return getFrontierFlipped() ^ getEdgeTypeBackToFrontier().causesFlip();
    }

    public void setAsCopy(RayMessage other) {
        super.setAsCopy(other);
        if (other.walkIds != null && other.walkIds.size() > 0) {
            getWalkIds().setAsCopy(other.walkIds);
        }
        if (other.walkOffsets != null && other.walkOffsets.size() > 0) {
            getWalkOffsets().clear();
            getWalkOffsets().addAll(other.walkOffsets); // Integer type is immutable; safe for references
        }
        walkLength = other.walkLength;
        if (other.accumulatedWalkKmer != null && other.accumulatedWalkKmer.getKmerLetterLength() > 0) {
            getAccumulatedWalkKmer().setAsCopy(other.accumulatedWalkKmer);
        }
        candidateToFrontierEdgeType = other.candidateToFrontierEdgeType;
        frontierFlipped = other.frontierFlipped;
        if (other.toScoreId != null && other.toScoreId.getKmerLetterLength() > 0) {
            getToScoreId().setAsCopy(other.toScoreId);
        }
        if (other.toScoreKmer != null && other.toScoreKmer.getKmerLetterLength() > 0) {
            getToScoreKmer().setAsCopy(other.toScoreKmer);
        }
        if (other.singleEndScores != null && other.singleEndScores.size() > 0) {
            getSingleEndScores().setAsCopy(other.singleEndScores);
        }
        if (other.pairedEndScores != null && other.pairedEndScores.size() > 0) {
            getPairedEndScores().setAsCopy(other.pairedEndScores);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        messageType = RayMessageType.values[in.readByte()];
        if ((messageFields & FIELDS.WALK) != 0) {
            getWalkIds().readFields(in);
            readWalkOffsets(in);
            setWalkLength(in.readInt());
            getAccumulatedWalkKmer().readFields(in);
        }
        if ((messageFields & FIELDS.EDGETYPE_BACK_TO_FRONTIER) != 0) {
            candidateToFrontierEdgeType = EDGETYPE.fromByte(in.readByte());
        }
        if ((messageFields & FIELDS.FRONTIER_FLIPPED) != 0) {
            frontierFlipped = in.readBoolean();
        }
        if ((messageFields & FIELDS.KMER_TO_SCORE) != 0) {
            getToScoreKmer().readFields(in);
            getToScoreId().readFields(in);
        }
        if ((messageFields & FIELDS.SCORE_SINGLE_END) != 0) {
            getSingleEndScores().readFields(in);
        }
        if ((messageFields & FIELDS.SCORE_PAIRED_END) != 0) {
            getPairedEndScores().readFields(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeByte((byte) messageType.ordinal());
        if (walkIds != null && walkIds.size() > 0) {
            walkIds.write(out);
            writeWalkOffsets(out);
            out.writeInt(getWalkLength());
            getAccumulatedWalkKmer().write(out);
        }
        if (candidateToFrontierEdgeType != null) {
            out.writeByte(candidateToFrontierEdgeType.get());
        }
        if (frontierFlipped != null) {
            out.writeBoolean(frontierFlipped);
        }
        if (toScoreKmer != null && toScoreKmer.getKmerLetterLength() > 0) {
            toScoreKmer.write(out);
            toScoreId.write(out);
        }
        if (singleEndScores != null && singleEndScores.size() > 0) {
            singleEndScores.write(out);
        }
        if (pairedEndScores != null && pairedEndScores.size() > 0) {
            pairedEndScores.write(out);
        }
    }

    @Override
    protected byte getActiveMessageFields() {
        byte fields = super.getActiveMessageFields();
        if (walkIds != null && walkIds.size() > 0) {
            fields |= FIELDS.WALK;
        }
        if (candidateToFrontierEdgeType != null) {
            fields |= FIELDS.EDGETYPE_BACK_TO_FRONTIER;
        }
        if (frontierFlipped != null) {
            fields |= FIELDS.FRONTIER_FLIPPED;
        }
        if (toScoreKmer != null && toScoreKmer.getKmerLetterLength() > 0) {
            fields |= FIELDS.KMER_TO_SCORE;
        }
        if (singleEndScores != null && singleEndScores.size() > 0) {
            fields |= FIELDS.SCORE_SINGLE_END;
        }
        if (pairedEndScores != null && pairedEndScores.size() > 0) {
            fields |= FIELDS.SCORE_PAIRED_END;
        }
        return fields;
    }

    private void readWalkOffsets(DataInput in) throws IOException {
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            getWalkOffsets().add(new Integer(in.readInt()));
        }
    }

    private void writeWalkOffsets(DataOutput out) throws IOException {
        out.writeInt(getWalkOffsets().size());
        for (Integer val : getWalkOffsets()) {
            out.writeInt(val);
        }
    }

    protected class FIELDS extends MESSAGE_FIELDS {
        public static final byte WALK = 1 << 1;
        public static final byte EDGETYPE_BACK_TO_FRONTIER = 1 << 2;
        public static final byte FRONTIER_FLIPPED = 1 << 3;
        public static final byte KMER_TO_SCORE = 1 << 4;
        public static final byte SCORE_SINGLE_END = 1 << 5;
        public static final byte SCORE_PAIRED_END = 1 << 6;
    }

    public RayMessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(RayMessageType type) {
        this.messageType = type;
    }

    public VKmerList getWalkIds() {
        if (walkIds == null) {
            walkIds = new VKmerList();
        }
        return walkIds;
    }

    public void setWalkIds(VKmerList walkIds) {
        this.walkIds = walkIds;
    }

    public ArrayList<Integer> getWalkOffsets() {
        if (walkOffsets == null) {
            walkOffsets = new ArrayList<>();
        }
        return walkOffsets;
    }

    public void setWalkOffsets(ArrayList<Integer> walkOffsets) {
        this.walkOffsets = walkOffsets;
    }

    public int getWalkLength() {
        return walkLength;
    }

    public void setWalkLength(int walkLength) {
        this.walkLength = walkLength;
    }

    public EDGETYPE getEdgeTypeBackToFrontier() {
        return candidateToFrontierEdgeType;
    }

    public void setEdgeTypeBackToFrontier(EDGETYPE edgeTypeBackToFrontier) {
        this.candidateToFrontierEdgeType = edgeTypeBackToFrontier;
    }

    public boolean getFrontierFlipped() {
        return this.frontierFlipped;
    }

    public void setFrontierFlipped(boolean frontierFlipped) {
        this.frontierFlipped = frontierFlipped;
    }

    public VKmer getToScoreKmer() {
        if (toScoreKmer == null)
            toScoreKmer = new VKmer();
        return toScoreKmer;
    }

    public void setToScoreKmer(VKmer toScoreKmer) {
        this.toScoreKmer = toScoreKmer;
    }

    public VKmer getToScoreId() {
        if (toScoreId == null) {
            toScoreId = new VKmer();
        }
        return toScoreId;
    }

    public void setToScoreId(VKmer toScoreId) {
        this.toScoreId = toScoreId;
    }

    public RayScores getSingleEndScores() {
        if (singleEndScores == null) {
            singleEndScores = new RayScores();
        }
        return singleEndScores;
    }

    public void setSingleEndScores(RayScores scores) {
        this.singleEndScores = scores;
    }

    public RayScores getPairedEndScores() {
        if (pairedEndScores == null) {
            pairedEndScores = new RayScores();
        }
        return pairedEndScores;
    }

    public void setPairedEndScores(RayScores scores) {
        this.pairedEndScores = scores;
    }

    public VKmer getAccumulatedWalkKmer() {
        if (accumulatedWalkKmer == null) {
            accumulatedWalkKmer = new VKmer();
        }
        return accumulatedWalkKmer;
    }

    public void setAccumulatedWalkKmer(VKmer accumulatedKmer) {
        this.accumulatedWalkKmer = accumulatedKmer;
    }

    @Override
    public void reset() {
        super.reset();
        walkIds = null;
        walkOffsets = null;
        accumulatedWalkKmer = null;
        candidateToFrontierEdgeType = null;
        frontierFlipped = null;
        toScoreKmer = null;
        singleEndScores = null;
        pairedEndScores = null;
    }

}
