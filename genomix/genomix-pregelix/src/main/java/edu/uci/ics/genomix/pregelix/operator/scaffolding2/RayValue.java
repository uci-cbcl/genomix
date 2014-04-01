package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import edu.uci.ics.genomix.data.types.EDGETYPE;
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
    HashMap<VKmer, ArrayList<RayMessage>> candidateMsgsMap =  null;
    ArrayList<Entry<EDGETYPE, VKmer>> forwardEdgesToKeep  =  null;
    ArrayList<Entry<EDGETYPE, VKmer>> reverseEdgesToKeep  =  null;
    //ArrayList<RayMessage> candidateMsgs = null;

    protected static class FIELDS {
        public static final short DIR_VS_INITIAL = 0b1 << 1;
        public static final short VISITED_LIST = 0b1 << 2;
        public static final short INTERSECTION = 0b1 << 3;
        public static final short STOP_SEARCH = 0b1 << 4;
        public static final short PENDING_CANDIDATE_BRANCHES = 1 << 5;
        public static final short  CANDIDATE_MSGS_MAP = 1 << 6;
		public static final short FORWARD_EDGES_TO_KEEP = 1 << 7;
		public static final short REVERSE_EDGES_TO_KEEP = 1 << 8;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if ((state & FIELDS.DIR_VS_INITIAL) != 0) {
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
            	VKmer key = new VKmer();
            	key.readFields(in);
            	boolean value = in.readBoolean();
            	getFlippedFromInitDir().put(key, value);
            }   
        }
        //visited = ((state & FIELDS.VISITED) != 0);
        if ((state & FIELDS.INTERSECTION) != 0) {
            int count = in.readInt();
            for (int i = 0; i < count; i++){
            	VKmer key = new VKmer();
            	key.readFields(in);
            	boolean value = in.readBoolean();
            	getIntersection().put(key, value);
            	
            }    
        }
        if ((state & FIELDS.STOP_SEARCH) != 0) {
            int count = in.readInt();
            for (int i = 0; i < count; i++){
            	VKmer key = new VKmer();
            	key.readFields(in);
            	boolean value = in.readBoolean();
            	getStopSearch().put(key, value);
            	
            }  
        }
        if ((state & FIELDS.VISITED_LIST) != 0) {
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                VKmer m = new VKmer();
                m.readFields(in);
                getVisitedList().add(m);
            }
        }
        if ((state & FIELDS.PENDING_CANDIDATE_BRANCHES) != 0) {
            int count = in.readInt();
            for (int i = 0; i < count; i++){
            	VKmer key = new VKmer();
            	key.readFields(in);
            	int value = in.readInt();
            	getPendingCandiateBranchesMap().put(key, value);
            	
            }   
        }
        /*
        getCandidateMsgs().clear();
        if ((state & FIELDS.CANDIDATE_MSGS) != 0) {
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                RayMessage m = new RayMessage();
                m.readFields(in);
                getCandidateMsgs.add(m);
            }
        }
        */
        if ((state & FIELDS.CANDIDATE_MSGS_MAP) != 0) {
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
            	VKmer key = new VKmer();
            	key.readFields(in);
            	int aCount = in.readInt();
            	ArrayList<RayMessage> msgs = new ArrayList<>();
            	for (int j = 0; j < aCount; j++) {
                    RayMessage m = new RayMessage();
                    m.readFields(in);
                    msgs.add(m);
                }
            	getCandidateMsgsMap().put(key, msgs);
            }
        }
        
        if ((state & FIELDS.FORWARD_EDGES_TO_KEEP) != 0) {
        	int count = in.readInt();
        	for (int i = 0; i < count; i++) {
        		EDGETYPE et = EDGETYPE.fromByte(in.readByte());
        		VKmer kmer = new VKmer();
        		kmer.readFields(in);
        		getForwardEdgesToKeep().add(new SimpleEntry<EDGETYPE, VKmer>(et, kmer));
        	}
        }
        if ((state & FIELDS.REVERSE_EDGES_TO_KEEP) != 0) {
        	int count = in.readInt();
        	for (int i = 0; i < count; i++) {
        		EDGETYPE et = EDGETYPE.fromByte(in.readByte());
        		VKmer kmer = new VKmer();
        		kmer.readFields(in);
        		getReverseEdgesToKeep().add(new SimpleEntry<EDGETYPE, VKmer>(et, kmer));
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
        if (candidateMsgsMap != null && candidateMsgsMap.size() > 0) {
            state |= FIELDS.CANDIDATE_MSGS_MAP;
        }
        if (forwardEdgesToKeep != null && forwardEdgesToKeep.size() > 0) {
            state |= FIELDS.FORWARD_EDGES_TO_KEEP;
        }
        if (reverseEdgesToKeep != null && reverseEdgesToKeep.size() > 0) {
            state |= FIELDS.REVERSE_EDGES_TO_KEEP;
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
        /**
        if (candidateMsgs != null && candidateMsgs.size() > 0) {
            out.writeInt(candidateMsgs.size());
            for (RayMessage m : candidateMsgs) {
                m.write(out);
            }
        }
        **/
        if (candidateMsgsMap != null && candidateMsgsMap.size() > 0) {
            out.writeInt(candidateMsgsMap.size());
            for (Entry<VKmer, ArrayList<RayMessage>> entry : candidateMsgsMap.entrySet()){
    			entry.getKey().write(out);
    			out.writeInt(entry.getValue().size());
    			for (RayMessage m : entry.getValue()) {
                    m.write(out);
                }
    		}
        }
        if (forwardEdgesToKeep != null && forwardEdgesToKeep.size() > 0) {
            out.writeInt(forwardEdgesToKeep.size());
            for (Entry<EDGETYPE, VKmer> entry : forwardEdgesToKeep){
            	out.writeByte(entry.getKey().get());
            	entry.getValue().write(out);
    		}
        }
        if (reverseEdgesToKeep != null && reverseEdgesToKeep.size() > 0) {
            out.writeInt(reverseEdgesToKeep.size());
            for (Entry<EDGETYPE, VKmer> entry : reverseEdgesToKeep){
            	out.writeByte(entry.getKey().get());
            	entry.getValue().write(out);
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
        candidateMsgsMap = null;
        forwardEdgesToKeep = null;
        reverseEdgesToKeep = null;
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
    /**
    public ArrayList<RayMessage> getCandidateMsgs() {
        if (candidateMsgs == null) {
            candidateMsgs = new ArrayList<>();
        }
        return candidateMsgs;
    }

    public void setCandidateMsgs(ArrayList<RayMessage> candidateMsgs) {
        this.candidateMsgs = candidateMsgs;
    }
     * @return 
    **/
    
    public HashMap<VKmer, ArrayList<RayMessage>> getCandidateMsgsMap(){
    	if (candidateMsgsMap == null){
    		candidateMsgsMap = new HashMap<VKmer, ArrayList<RayMessage>>();
    	}
    	return candidateMsgsMap;
    }
    
    public void setCandidateMsgsMap(HashMap<VKmer, ArrayList<RayMessage>> candidateMsgs){
    	this.candidateMsgsMap= candidateMsgs;
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
    
    public ArrayList<Entry<EDGETYPE, VKmer>> getForwardEdgesToKeep() {
    	if (forwardEdgesToKeep == null) {
    		forwardEdgesToKeep = new ArrayList<>();
    	}
		return forwardEdgesToKeep;
	}
    
    public void setForwardEdgesToKeep(ArrayList<Entry<EDGETYPE, VKmer>> forwardEdgesToKeep) {
    	this.forwardEdgesToKeep = forwardEdgesToKeep;
	}
    
    public ArrayList<Entry<EDGETYPE, VKmer>> getReverseEdgesToKeep() {
    	if (reverseEdgesToKeep == null) {
    		reverseEdgesToKeep = new ArrayList<>();
    	}
		return reverseEdgesToKeep;
	}
    
    public void setReverseEdgesToKeep(ArrayList<Entry<EDGETYPE, VKmer>> reverseEdgesToKeep) {
    	this.reverseEdgesToKeep = reverseEdgesToKeep;
	}
    
}
