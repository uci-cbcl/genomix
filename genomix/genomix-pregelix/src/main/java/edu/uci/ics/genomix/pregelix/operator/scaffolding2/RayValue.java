package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.ReadHeadInfo;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;

public class RayValue extends VertexValueWritable {
    private static final long serialVersionUID = 1L;

    HashMap<VKmer, Boolean> flippedFromInitialDirection = null;
    List<VKmer> visitedList;
    HashMap<VKmer,Boolean> intersection = null;
    HashMap<VKmer, Boolean> stopSearch = null;
    HashMap<VKmer, Integer> pendingCandidateBranchesMap = null;
    ArrayList<RayMessage> candidateMsgs = null;

    protected static class FIELDS {
        public static final byte DIR_VS_INITIAL = 0b1 << 1;
        public static final byte VISITED_LIST = 0b1 << 2;
        public static final byte INTERSECTION = 0b1 << 3;
        public static final byte STOP_SEARCH = 0b1 << 4;
        public static final byte PENDING_CANDIDATE_BRANCHES = 1 << 5;
        public static final byte CANDIDATE_MSGS = 1 << 6;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if ((state & FIELDS.DIR_VS_INITIAL) != 0) {
            getIntersection().clear();
            int count = in.readInt();
            for (int i = 0; i < count; i++){
            	VKmer key = new VKmer();
            	key.readFields(in);
            	boolean value = in.readBoolean();
            	flippedFromInitialDirection.put(key, value);
            	
            }   
        }
        //visited = ((state & FIELDS.VISITED) != 0);      
        if ((state & FIELDS.INTERSECTION) != 0) {
            getIntersection().clear();
            int count = in.readInt();
            for (int i = 0; i < count; i++){
            	VKmer key = new VKmer();
            	key.readFields(in);
            	boolean value = in.readBoolean();
            	intersection.put(key, value);
            	
            }    
        }
        if ((state & FIELDS.STOP_SEARCH) != 0) {
            getStopSearch().clear();
            int count = in.readInt();
            for (int i = 0; i < count; i++){
            	VKmer key = new VKmer();
            	key.readFields(in);
            	boolean value = in.readBoolean();
            	stopSearch.put(key, value);
            	
            }  
        }
        
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
            getPendingCandiateBranchesMap().clear();
            int count = in.readInt();
            for (int i = 0; i < count; i++){
            	VKmer key = new VKmer();
            	key.readFields(in);
            	int value = in.readInt();
            	pendingCandidateBranchesMap.put(key, value);
            	
            }   
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
            state |=  FIELDS.DIR_VS_INITIAL;
        }
        if (intersection != null) {
            state |= FIELDS.INTERSECTION;
        }
        if (stopSearch != null) {
            state |= FIELDS.STOP_SEARCH;
        }
        
        if (visitedList != null && visitedList.size() > 0) {
            state |= FIELDS.VISITED_LIST;
        } 
        
        if (pendingCandidateBranchesMap != null) {
            state |= FIELDS.PENDING_CANDIDATE_BRANCHES;
        }
        if (candidateMsgs != null && candidateMsgs.size() > 0) {
            state |= FIELDS.CANDIDATE_MSGS;
        }
        super.write(out);
        
        if (flippedFromInitialDirection != null) {
            out.writeInt(flippedFromInitialDirection.size());
            for (Entry<VKmer, Boolean> entry : flippedFromInitialDirection.entrySet()){
    			entry.getKey().write(out);
    			out.writeBoolean(entry.getValue());
    		}
        } 
        if (intersection != null) {
            out.writeInt(intersection.size());
            for (Entry<VKmer, Boolean> entry : intersection.entrySet()){
    			entry.getKey().write(out);
    			out.writeBoolean(entry.getValue());
    		}
        } 
        if (stopSearch != null) {
            out.writeInt(stopSearch.size());
            for (Entry<VKmer, Boolean> entry : stopSearch.entrySet()){
    			entry.getKey().write(out);
    			out.writeBoolean(entry.getValue());
    		}
        }
        if (visitedList != null && visitedList.size() > 0) {
            out.writeInt(visitedList.size());
            for (VKmer m : visitedList) {
                m.write(out);
            }
        }
        if (pendingCandidateBranchesMap != null) {
            out.writeInt(pendingCandidateBranchesMap.size());
            for (Entry<VKmer, Integer> entry : pendingCandidateBranchesMap.entrySet()){
    			entry.getKey().write(out);
    			out.writeInt(entry.getValue());
    		}
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
        intersection = null;
        stopSearch = null;
        visitedList = null;
        pendingCandidateBranchesMap = null;
        candidateMsgs = null;
    }

    /**
     * @return whether or not I have any readids that **could** contribute to the current walk
     */
    public boolean isOutOfRange(int myOffset, int walkLength, int maxDist, VKmer seed) {
        int numBasesToSkip = Math.max(0, walkLength - maxDist - myOffset);
        int myLength = getKmerLength() - Kmer.getKmerLength() + 1;
        if (!flippedFromInitialDirection.get(seed)) {
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
    
    public HashMap<VKmer,Integer> getPendingCandiateBranchesMap(){
    	if (pendingCandidateBranchesMap == null){
    		pendingCandidateBranchesMap = new HashMap<VKmer, Integer>();
    	}
    	return pendingCandidateBranchesMap;
    }
    public void setPendingCandidateBranchesMap(HashMap<VKmer, Integer> pendingCandidateBranchesMap){
    	this.pendingCandidateBranchesMap = pendingCandidateBranchesMap;
    }
    
    public HashMap<VKmer, Boolean> getIntersection(){
    	if (intersection == null){
    		intersection = new HashMap<VKmer, Boolean>();
    	}
    	return intersection;
    }
    public void setIntersection(HashMap<VKmer, Boolean> intersection){
    	this.intersection = intersection;
    }
    
    public HashMap<VKmer, Boolean> getStopSearch(){
    	if (stopSearch == null){
    		stopSearch = new HashMap<VKmer, Boolean>();
    	}
    	return stopSearch;
    }
    public void setStopSearch(HashMap<VKmer, Boolean> stopSearch){
    	this.stopSearch= stopSearch;
    }
    
    public HashMap<VKmer, Boolean> getFlippedFromInitDir(){
    	if (flippedFromInitialDirection == null){
    		flippedFromInitialDirection = new HashMap<VKmer, Boolean>();
    	}
    	return flippedFromInitialDirection;
    }
    public void setFlippedFromInitDir(HashMap<VKmer, Boolean> flippedFromInitialDirection){
    	this.flippedFromInitialDirection= flippedFromInitialDirection;
    }
}
