package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.ArrayList;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;


public class RayScaffoldingMessage extends MessageWritable{
	private ArrayList<VKmer> walk;
	private boolean startFlag, computeFlag, previsitedFlag, neighborFlag,computeRulesFlag;
	private VKmer kmer, lastVertex;
	private int walkSize;
	private int index;
	private int offset;
	private int ruleA, ruleB, ruleC;
	
	public RayScaffoldingMessage() {
        super();
        walk = new ArrayList<VKmer>();
    }
	
	public void reset() {
        super.reset();
        walk.clear();
    }
	
	
	public ArrayList<VKmer> getWalk() {
		return walk;
	}
	
	public void setWalk(ArrayList<VKmer> walk) {
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
}


