package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.vertex.VertexValueWritable;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;


public class RayScaffoldingMessage extends MessageWritable{
	
    protected class RAYSCAFFOLDING_MESSAGE_FIELDS extends MESSAGE_FIELDS {
        public static final byte WALK = 1 << 1; 
        public static final byte KMER = 1 << 2; 
        public static final byte LAST_VERTEX = 1 << 3; 
        public static final byte EDGETYPE = 1 << 4;    
    }
    //Too many things here! You need to modify them! 
    
	private VKmerList walk;
	private boolean startFlag, computeFlag, previsitedFlag, neighborFlag ,computeRulesFlag;
	private boolean doneFlag, removeEdgesFlag, flipFlag;
	private VKmer kmer, lastVertex;
	private int walkSize;
	private int index;
	private int offset;
	private int ruleA, ruleB, ruleC;
	private EDGETYPE edgeType;
	
	public RayScaffoldingMessage() {
        super();
        walk = new VKmerList();
        kmer = new VKmer();
        //I don't know how to define it
        edgeType = EDGETYPE.FF;
        lastVertex = new VKmer();
    }
	
	public void reset() {
        super.reset();
        if (walk == null) {
	    	walk = new VKmerList();
	    } else{
	    	walk.clear();
	    }
        //I don't know how to reset it
        edgeType = EDGETYPE.FF;
        lastVertex.reset(3);
    }
	
	
	public VKmerList getWalk() {
		return walk;
	}
	
	public void setWalk(VKmerList walk) {
        this.walk = walk;
    }
	
	public void setStartFlag() {
        this.startFlag = true;
    }
	
	public boolean getStartFlag(){
		return this.startFlag;
	}
	
	public void setKmer(VKmer internalKmer){
		kmer = internalKmer;
	}
	
	public VKmer getKmer(){
		return this.kmer;
	}
	
	public void setComputeFlag(){
		this.computeFlag = true;
	}
	
	public boolean getComputeFlag(){
		return this.computeFlag;
	}
	
	public void setWalkSize(int size){
		this.walkSize = size;
	}
	
	public void setIndex(int index){
		this.index = index;
	}
	
	public int getWalkSize(){
		return this.walkSize;
	}
	
	public int getIndex(){
		return this.index ;
	}
	
	public void setOffset(int offsetFromOneKmer){
		this.offset = offsetFromOneKmer;
	}
	
	public int getOffset(){
		return this.offset;
	}
	
	public void setRules(int ruleA, int ruleB, int ruleC){
		this.ruleA = ruleA;
		this.ruleC = ruleC;
		this.ruleB = ruleB;
	}
	
	public int getRuleA(){
		return this.ruleA; 
	}
	
	public int getRuleB(){
		return this.ruleB; 
	}
	
	public int getRuleC(){
		return this.ruleC; 
	}

	public VKmer getLastVertex(){
		return this.lastVertex;
	}
	
	public void setLastVertex(VKmer lastVer){
		this.lastVertex = lastVer;
	}
	
	public void setPrevisitedFlag(){
		this.previsitedFlag = true;
	}
	
	public boolean getPrevisitedFlag(){
		return this.previsitedFlag;
	}
	
	public void setNeighborFlag() {
        this.neighborFlag = true;
    }
	
	public boolean getNeighborFlag(){
		return this.neighborFlag;
	}
	
	public void setComputeRulesFlag(){
		this.computeRulesFlag = true;
	}
	
	public boolean getComputeRulesFlag(){
		return this.computeRulesFlag;
	}
	public void setDoneFlag(){
		this.doneFlag = true;
	}
	
	public boolean getDoneFlag(){
		return this.doneFlag;
	}
	
	public void setRemoveEdgesFlag(){
		this.removeEdgesFlag = true;
	}
	
	public boolean getRemoveEdgesFlag(){
		return this.removeEdgesFlag;
	}
	
	public void setEdgeType(EDGETYPE et){
		this.edgeType = et;
	}
	
	public EDGETYPE getEdgeType(){
		return this.edgeType;
	}
	
	public void setFlipFlag(){
		this.flipFlag = true;
	}
	
	public boolean getFlipFlag(){
		return this.flipFlag;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		//int walkLength = in.readInt();
		if ((messageFields & RAYSCAFFOLDING_MESSAGE_FIELDS.WALK) != 0){
			walk.clear();
			walk.readFields(in);
		}
		startFlag = in.readBoolean();
		computeFlag = in.readBoolean();
		previsitedFlag = in.readBoolean();
		neighborFlag =  in.readBoolean();
		computeRulesFlag = in.readBoolean();
		doneFlag = in.readBoolean();
		removeEdgesFlag = in.readBoolean();
		flipFlag = in.readBoolean();
		if ((messageFields & RAYSCAFFOLDING_MESSAGE_FIELDS.KMER) != 0){
			kmer.readFields(in);
			}
		if ((messageFields & RAYSCAFFOLDING_MESSAGE_FIELDS.LAST_VERTEX) != 0){
			lastVertex.readFields(in);
			}
		walkSize = in.readInt();
		index = in.readInt();
		offset = in.readInt();
		ruleA = in.readInt();
		ruleB = in.readInt();
		ruleC = in.readInt();
		if ((messageFields & RAYSCAFFOLDING_MESSAGE_FIELDS.EDGETYPE) != 0){
			edgeType = EDGETYPE.fromByte(in.readByte());
			}
		
//		private EDGETYPE edgeType = null;
	}
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		//out.writeInt(walk.size());
		if( walk != null){
			walk.write(out);
		}
		out.writeBoolean(startFlag);
		out.writeBoolean(computeFlag);
		out.writeBoolean(previsitedFlag);
		out.writeBoolean(neighborFlag);
		out.writeBoolean(computeRulesFlag);
		out.writeBoolean(doneFlag);
		out.writeBoolean(removeEdgesFlag);
		out.writeBoolean(flipFlag);
		if (kmer != null){
			kmer.write(out);
		}
		if (lastVertex != null){
			lastVertex.write(out);
		}
		out.writeInt(walkSize);
		out.writeInt(index);
		out.writeInt(offset);
		out.writeInt(ruleA);
		out.writeInt(ruleB);
		out.writeInt(ruleC);
		if (edgeType != null){
			out.writeByte(edgeType.get());
		}
		
	}
	
	@Override
    protected byte getActiveMessageFields() {
        byte messageFields = super.getActiveMessageFields();
        if (walk != null) {
            messageFields |= RAYSCAFFOLDING_MESSAGE_FIELDS.WALK;
        }
        if (kmer != null) {
            messageFields |= RAYSCAFFOLDING_MESSAGE_FIELDS.KMER;
        }
        if (lastVertex != null) {
            messageFields |= RAYSCAFFOLDING_MESSAGE_FIELDS.LAST_VERTEX;
        }
        if (edgeType != null) {
            messageFields |= RAYSCAFFOLDING_MESSAGE_FIELDS.EDGETYPE;
        }
        return messageFields;
    }
}

