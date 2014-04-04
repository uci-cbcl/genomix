package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.types.VKmer;

public class RayVertexInfo implements Writable{
    Boolean flippedFromInitialDirection = false;
    boolean intersection = false;
    boolean stopSearch = false;
    ArrayList<VKmer> pendingCandidateBranches = null;
    ArrayList<RayMessage> candidateMsgs = null;
    
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		flippedFromInitialDirection = in.readBoolean();
		intersection = in.readBoolean();
		stopSearch = in.readBoolean();
		if(getCandidateMsgs() != null){
			getCandidateMsgs().clear();
			int count = in.readInt();
			for (int i = 0; i < count; i++) {
				RayMessage m = new RayMessage();
				m.readFields(in);
				candidateMsgs.add(m);
			}
        }
		if (getPendingCandidateBranches() !=  null){
			getPendingCandidateBranches().clear();
			int count = in.readInt();
			for (int i = 0; i < count; i++) {
				VKmer m = new VKmer();
				m.readFields(in);
				pendingCandidateBranches.add(m);
        }
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeBoolean(flippedFromInitialDirection);
		out.writeBoolean(intersection);
		out.writeBoolean(stopSearch);
		if (candidateMsgs != null && candidateMsgs.size() > 0) {
            out.writeInt(candidateMsgs.size());
            for (RayMessage m : candidateMsgs) {
                m.write(out);
            }
        }
		if (pendingCandidateBranches != null) {
            out.writeInt(pendingCandidateBranches.size());
            for (VKmer m : pendingCandidateBranches) {
                m.write(out);
            }
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
    
    public ArrayList<VKmer> getPendingCandidateBranches() {
        if (pendingCandidateBranches == null) {
        	pendingCandidateBranches = new ArrayList<>();
        }
        return pendingCandidateBranches;
    }

    public void setPendingCandidateBranches(ArrayList<VKmer> pendingCandidates) {
        this.pendingCandidateBranches = pendingCandidates;
    }
	
}
