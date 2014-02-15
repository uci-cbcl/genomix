package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.ReadHeadInfo;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;

public class RayValue extends VertexValueWritable {
    private static final long serialVersionUID = 1L;

    Boolean flippedFromInitialDirection = null;
    //boolean visited = false;
    List<VKmer> visitedList;
    boolean intersection = false;
    boolean stopSearch = false;
    
    HashMap<VKmer, Integer> pendingCandidateBranchesMap = null;
    Integer pendingCandidateBranchesCopy = null;
    Integer pendingCandidateBranches = null;
    ArrayList<RayMessage> candidateMsgs = null;

    protected static class FIELDS {
        public static final byte DIR_FLIPPED_VS_INITIAL = 0b01;
        public static final byte DIR_SAME_VS_INITIAL = 0b10;
        public static final byte VISITED_LIST = 0b1 << 2;
        public static final byte INTERSECTION = 0b1 << 3;
        public static final byte STOP_SEARCH = 0b1 << 4;
        public static final byte PENDING_CANDIDATE_BRANCHES = 1 << 5;
        public static final byte CANDIDATE_MSGS = 1 << 6;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if (((state) & FIELDS.DIR_FLIPPED_VS_INITIAL) != 0) {
            flippedFromInitialDirection = true;
        } else if (((state) & FIELDS.DIR_SAME_VS_INITIAL) != 0) {
            flippedFromInitialDirection = false;
        } else {
            flippedFromInitialDirection = null;
        }
        //visited = ((state & FIELDS.VISITED) != 0);        
        intersection = ((state & FIELDS.INTERSECTION) != 0);
        stopSearch = ((state & FIELDS.STOP_SEARCH) != 0);
        
        if ((state & FIELDS.VISITED_LIST) != 0) {
            getVisitedList().clear();
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                VKmer m = new VKmer();
                m.readFields(in);
                visitedList.add(m);
            }
        }
        
        if ((state & FIELDS.PENDING_CANDIDATE_BRANCHES) != 0) {
            pendingCandidateBranches = in.readInt();
            pendingCandidateBranchesCopy = in.readInt();
        }
        if ((state & FIELDS.CANDIDATE_MSGS) != 0) {
            getCandidateMsgs().clear();
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                RayMessage m = new RayMessage();
                m.readFields(in);
                candidateMsgs.add(m);
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        state = 0;
        if (flippedFromInitialDirection != null) {
            state |= flippedFromInitialDirection ? FIELDS.DIR_FLIPPED_VS_INITIAL : FIELDS.DIR_SAME_VS_INITIAL;
        }
        if (intersection) {
            state |= FIELDS.INTERSECTION;
        }
        if (stopSearch) {
            state |= FIELDS.STOP_SEARCH;
        }
        
        if (visitedList != null && visitedList.size() > 0) {
            state |= FIELDS.VISITED_LIST;
        } 
        
        if (pendingCandidateBranches != null) {
            state |= FIELDS.PENDING_CANDIDATE_BRANCHES;
        }
        if (candidateMsgs != null && candidateMsgs.size() > 0) {
            state |= FIELDS.CANDIDATE_MSGS;
        }
        super.write(out);
        
        if (visitedList != null && visitedList.size() > 0) {
            out.writeInt(visitedList.size());
            for (VKmer m : visitedList) {
                m.write(out);
            }
        }
        
        if (pendingCandidateBranches != null) {
            out.writeInt(pendingCandidateBranches);
            out.writeInt(pendingCandidateBranchesCopy);
        }
        if (candidateMsgs != null && candidateMsgs.size() > 0) {
            out.writeInt(candidateMsgs.size());
            for (RayMessage m : candidateMsgs) {
                m.write(out);
            }
        }
    }

    @Override
    public void reset() {
        super.reset();
        flippedFromInitialDirection = null;
        intersection = false;
        stopSearch = false;
        visitedList = null;
        pendingCandidateBranches = null;
        pendingCandidateBranchesCopy = null;
        candidateMsgs = null;
    }

    /**
     * @return whether or not I have any readids that **could** contribute to the current walk
     */
    public boolean isOutOfRange(int myOffset, int walkLength, int maxDist) {
        int numBasesToSkip = Math.max(0, walkLength - maxDist - myOffset);
        int myLength = getKmerLength() - Kmer.getKmerLength() + 1;
        if (!flippedFromInitialDirection) {
            // TODO fix max offset to be distance
            // cut off the beginning
            if (numBasesToSkip > myLength) {
                // start > offsets I contain-- no valid readids
                return true;
            }
            return getUnflippedReadIds().getOffSetRange(numBasesToSkip, ReadHeadInfo.MAX_OFFSET_VALUE).isEmpty();
        } else {
            // cut off the end 
            if (myLength - numBasesToSkip < 0) {
                // my max is negative-- no valid readids
                return true;
            }
            //FIXME
            return getFlippedReadIds().getOffSetRange(0, Math.max(0, getKmerLength() - numBasesToSkip)).isEmpty();
            //return getFlippedReadIds().getOffSetRange(0, myLength - numBasesToSkip).isEmpty();
        }
    }

    public ArrayList<RayMessage> getCandidateMsgs() {
        if (candidateMsgs == null) {
            candidateMsgs = new ArrayList<>();
        }
        return candidateMsgs;
    }

    public void setCandidateMsgs(ArrayList<RayMessage> candidateMsgs) {
        this.candidateMsgs = candidateMsgs;
    }
    
    public List<VKmer> getVisitedList() {
        if (visitedList == null) {
            visitedList = new ArrayList<>();
        }
        return visitedList;
    }

    public void setVisitedList(List<VKmer> visitedList) {
        this.visitedList = visitedList;
    }
    
}
