package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.sun.org.apache.commons.logging.Log;

import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.scaffolding2.RayValue.FIELDS;

public class RayMessage extends MessageWritable {

    private RayMessageType messageType;

    public enum RayMessageType {
        REQUEST_CANDIDATE_KMER,
        ASSEMBLE_CANDIDATES,
        REQUEST_SCORE,
        AGGREGATE_SCORE,
        PRUNE_EDGE,
        ADD_EDGE,
        CONTINUE_WALK,
        STOP,
        UPDATE_FORK_COUNT;
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
    private ArrayList<RayScores> singleEndScores = null;
    private ArrayList<RayScores> pairedEndScores = null;

    /** for REQUEST_KMER */
    private Boolean candidateFlipped = false;
    private EDGETYPE edgeTypeBackToPrev = null;

    /** for UPDATE_FORK_COUNT */
    private Integer numberOfForks = null;

    /** for REQUEST_SCORE, AGGREGATE_SCORE */
    private Integer pathIndex = null; // the index of the path represented by this msg

    // TODO remove these whenever we want to (they're only used for debugging)
    public VKmerList candidatePathIds = new VKmerList();
    
    //Parallel Problem
    public VKmer seed = null;

    /** for early stop **/
    private HashMap<VKmer, Integer> visitCounter = null;
    
    public static Logger LOG = Logger.getLogger(RayMessage.class.getName());
    
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
    public void visitNode(VKmer id, RayValue vertex, DIR accumulatedKmerDir, VKmer seed) {
        getWalkIds().append(id);
        getWalkOffsets().add(getWalkLength());
        setWalkLength(getWalkLength() + vertex.getKmerLength() - Kmer.getKmerLength() + 1);
        if (vertex.getKmerLength() >= 100) {
        	LOG.info("found a long node: " + id + ", length: " + vertex.getKmerLength() + ", total walk length: " + getWalkLength());
        } else {
        	LOG.info("found a short node: " + id + ", length: " + vertex.getKmerLength() + ", total walk length: " + getWalkLength());
        }

        if (accumulatedWalkKmer == null || accumulatedWalkKmer.getKmerLetterLength() == 0) {
            // kmer oriented st always merge in an F dir (seed's offset always 0)
            setAccumulatedWalkKmer(accumulatedKmerDir == DIR.FORWARD ? vertex.getInternalKmer() : vertex
                    .getInternalKmer().reverse());
        } else {
            EDGETYPE accumulatedToVertexET = !vertex.getFlippedFromInitDir().get(seed) ? EDGETYPE.FF : EDGETYPE.FR;
            getAccumulatedWalkKmer().mergeWithKmerInDir(accumulatedToVertexET, Kmer.getKmerLength(),
                    vertex.getInternalKmer());
        }
    }

    public void setAsCopy(RayMessage other) {
        super.setAsCopy(other);
        messageType = other.messageType;
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
            getSingleEndScores().clear();
            for (RayScores s : other.singleEndScores) {
                getSingleEndScores().add(new RayScores(s));
            }
        }
        if (other.pairedEndScores != null && other.pairedEndScores.size() > 0) {
            getPairedEndScores().clear();
            for (RayScores s : other.pairedEndScores) {
                getPairedEndScores().add(new RayScores(s));
            }
        }
        candidateFlipped = other.candidateFlipped;
        edgeTypeBackToPrev = other.edgeTypeBackToPrev;
        numberOfForks = other.numberOfForks;
        pathIndex = other.pathIndex;
        candidatePathIds.setAsCopy(other.candidatePathIds);
        if (other.seed != null) {
            getSeed().setAsCopy(other.seed);
        }
        
        if(other.visitCounter != null && other.visitCounter.size() > 0){
        	getVisitCounter().clear();
            for (Entry<VKmer, Integer> entry : other.visitCounter.entrySet()){
    			getVisitCounter().put(entry.getKey(), entry.getValue());
    		}
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
            getSingleEndScores().clear();
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                RayScores r = new RayScores();
                r.readFields(in);
                getSingleEndScores().add(r);
            }
        }
        if ((messageFields & FIELDS.SCORE_PAIRED_END) != 0) {
            getPairedEndScores().clear();
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                RayScores r = new RayScores();
                r.readFields(in);
                getPairedEndScores().add(r);
            }
        }

        byte remainingFields = in.readByte();
        if ((remainingFields & FIELDS.CANDIDATE_FLIPPED) != 0) {
            candidateFlipped = in.readBoolean();
        }
        if ((remainingFields & FIELDS.EDGETYPE_BACK_TO_PREV) != 0) {
            edgeTypeBackToPrev = EDGETYPE.fromByte(in.readByte());
        }
        if ((remainingFields & FIELDS.NUMBER_OF_FORKS) != 0) {
            numberOfForks = in.readInt();
        }
        if ((remainingFields & FIELDS.PATH_INDEX) != 0) {
            pathIndex = in.readInt();
        }
        candidatePathIds.readFields(in);
       
        if ((remainingFields & FIELDS.SEED) != 0) {
            getSeed().readFields(in);
        }
        
        if ((remainingFields & FIELDS.VISIT_COUNTER) != 0) {
            getVisitCounter().clear();
            int count = in.readInt();
            for (int i = 0; i < count; i++){
            	VKmer key = new VKmer();
            	key.readFields(in);
            	int value = in.readInt();
            	visitCounter.put(key, value);
            	
            }   
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
            out.writeInt(singleEndScores.size());
            for (RayScores s : singleEndScores) {
                s.write(out);
            }
        }
        if (pairedEndScores != null && pairedEndScores.size() > 0) {
            out.writeInt(pairedEndScores.size());
            for (RayScores s : pairedEndScores) {
                s.write(out);
            }
        }

        byte remainingFields = (byte) ((seed != null ? FIELDS.SEED : 0)|(candidateFlipped != null ? FIELDS.CANDIDATE_FLIPPED : 0)
                | (edgeTypeBackToPrev != null ? FIELDS.EDGETYPE_BACK_TO_PREV : 0)
                | (numberOfForks != null ? FIELDS.NUMBER_OF_FORKS : 0) | (pathIndex != null ? FIELDS.PATH_INDEX : 0) | (visitCounter != null ? FIELDS.VISIT_COUNTER : 0));
        out.writeByte(remainingFields);
        if (candidateFlipped != null) {
            out.writeBoolean(candidateFlipped);
        }
        if (edgeTypeBackToPrev != null) {
            out.writeByte(edgeTypeBackToPrev.get());
        }
        if (numberOfForks != null) {
            out.writeInt(numberOfForks);
        }
        if (pathIndex != null) {
            out.writeInt(pathIndex);
        }
        candidatePathIds.write(out);

        if (seed != null) {
            seed.write(out);
        }
        if(visitCounter != null){
        	out.writeInt(visitCounter.size());
            for (Entry<VKmer, Integer> entry : visitCounter.entrySet()){
    			entry.getKey().write(out);
    			out.writeInt(entry.getValue());
    		}
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
        //if (seed != null){
        //	fields |= FIELDS.SEED;
        //}
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

        // stored as an additional byte
        public static final byte CANDIDATE_FLIPPED = 1 << 0;
        public static final byte EDGETYPE_BACK_TO_PREV = 1 << 1;
        public static final byte NUMBER_OF_FORKS = 1 << 2;
        public static final byte PATH_INDEX = 1 << 3;  
        public static final byte SEED = 1 << 4;
        public static final byte VISIT_COUNTER = 1 << 5;
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

    public ArrayList<RayScores> getSingleEndScores() {
        if (singleEndScores == null) {
            singleEndScores = new ArrayList<>();
        }
        return singleEndScores;
    }

    public void setSingleEndScores(ArrayList<RayScores> scores) {
        this.singleEndScores = scores;
    }

    public ArrayList<RayScores> getPairedEndScores() {
        if (pairedEndScores == null) {
            pairedEndScores = new ArrayList<>();
        }
        return pairedEndScores;
    }

    public void setPairedEndScores(ArrayList<RayScores> scores) {
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

    /**
     * @return whether the candidate node represented by this message is a flipped or unflipped node
     */
    public boolean getCandidateFlipped() {
        return candidateFlipped;
    }

    public void setCandidateFlipped(boolean flipped) {
        this.candidateFlipped = flipped;
    }

    public EDGETYPE getEdgeTypeBackToPrev() {
        return this.edgeTypeBackToPrev;
    }

    public void setEdgeTypeBackToPrev(EDGETYPE edgeTypeBackToPrev) {
        this.edgeTypeBackToPrev = edgeTypeBackToPrev;
    }

    public int getNumberOfForks() {
        return numberOfForks;
    }

    public void setNumberOfForks(int numberOfForks) {
        this.numberOfForks = numberOfForks;
    }

    public void setPathIndex(int pathIndex) {
        this.pathIndex = pathIndex;
    }

    public int getPathIndex() {
        return pathIndex;
    }
    
    public VKmer getSeed(){
    	if (seed == null){
    		seed = new VKmer();
    	}
    	return seed;
    }
    
    public void setSeed(VKmer seed){
    	this.seed = seed;
    }
    
    public HashMap<VKmer, Integer> getVisitCounter(){
    	if (visitCounter == null){
    		visitCounter = new HashMap<VKmer, Integer>();
    	}
    	return visitCounter;
    }
    public void setVisitCounter(HashMap<VKmer, Integer> visitCounter){
    	this.visitCounter= visitCounter;
    }
    @Override
    public void reset() {
        super.reset();
        walkIds = null;
        walkOffsets = null;
        walkLength = null;
        accumulatedWalkKmer = null;
        candidateToFrontierEdgeType = null;
        frontierFlipped = null;
        toScoreId = null;
        toScoreKmer = null;
        singleEndScores = null;
        pairedEndScores = null;
        candidateFlipped = null;
        edgeTypeBackToPrev = null;
        numberOfForks = null;
        pathIndex = null;
        candidatePathIds.clear();
        seed = null;
        visitCounter = null;
    }
}
