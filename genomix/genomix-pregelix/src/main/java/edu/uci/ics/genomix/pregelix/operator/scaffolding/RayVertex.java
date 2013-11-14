package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.type.VKmer;

public class RayVertex extends DeBruijnGraphCleanVertex<ScaffoldingVertexValueWritable, RayScaffoldingMessage>{
	public static int SCAFFOLDING_VERTEX_MIN_COVERAGE = -1;
	public int K = 21;
	//private ArrayList<VKmer> neighbors;
	//private boolean startPoint;
	//private boolean preVisited;
	// VKmer lastKmerInWalk;

	public void initVertex(){	
		///Complete this!
		if (SCAFFOLDING_VERTEX_MIN_COVERAGE < 0)
            SCAFFOLDING_VERTEX_MIN_COVERAGE = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.SCAFFOLDING_VERTEX_MIN_COVERAGE));
        
		if (outgoingMsg == null) {
            outgoingMsg = new RayScaffoldingMessage();
        } else {
            outgoingMsg.reset();
        }
        
    ////??
        //StatisticsAggregator.preGlobalCounters.clear();   
        //counters.clear();
        //getVertexValue().getCounters().clear();
    ////??	
	    if (getVertexValue().walk == null) {
	    	getVertexValue().walk = new ArrayList<VKmer>(); 
	    } else{
	    	getVertexValue().walk.clear();
	    }
	}

	//For each neighbor, which is an option to continue, find the offsets
	public void sendMsgToBranches(DIR direction, Iterator<RayScaffoldingMessage> msgIterator) {
		///need a method for this:
		getVertexValue().previsitedFlag = true;
		readWalk(msgIterator);
		getVertexValue().walk.add(getVertexId());
		//if there is just one neighbor?
		for (EDGETYPE et : direction.edgeTypes()){
			for (VKmer neighbor : getVertexValue().getEdgeMap(et).keySet()) {
				sendMsgToNeighbor(neighbor);	
			}
		}
	}
	
	//tell neighbor about the walk
	public void sendMsgToNeighbor(VKmer neighbor){
		outgoingMsg.setWalk(getVertexValue().walk);
		outgoingMsg.setNeighborFlag();
		
		//keep the decision point because we need to come back to it
		outgoingMsg.setLastVertex(getVertexId());
		sendMsg(neighbor ,outgoingMsg);
		
	}
	

	// for each Kmer in the walk and each BranchKmer finds the offset
	public int offset(VKmer neighbor){
		int offset = 0;
		for (ReadHeadInfo read : getVertexValue().getStartReads()){
			if (checkedDistance (read, neighbor)){
				offset++;
			}
		}
		return offset;
	}	
	
	public boolean checkedDistance(ReadHeadInfo read, VKmer neighbor){
		//if neighbor exist in read table
		//How to work with a read?
		read.getReadId();
		read.
		int neighborPositionOnRead = getVertexValue().walkSize - getVertexValue().index + read.indexOf(getVertexId().toString()) + 1;
		///I know!!
		if(read.indexOf(neighbor.toString()) == neighborPositionOnread ){
			return true;
		}else{
			return false;
		}
	}	
	
	
		//compute the rules , add new sentence in each step	
	public void rules_add(int offset){
		//int l = walkSize + K - 1;
		
		getVertexValue().rules_a = getVertexValue().rules_a +  (getVertexValue().walkSize - getVertexValue().index) * offset;	
		getVertexValue().rules_b = getVertexValue().rules_b + offset;
		if ((offset < getVertexValue().rules_c) && (offset!= 0)){
			getVertexValue().rules_c = offset;
		}
	}
	
	
	public void readWalk(Iterator<RayScaffoldingMessage> msgIterator){
		RayScaffoldingMessage incomingMsg;
        if (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            getVertexValue().walk = incomingMsg.getWalk();
        }
	}
	
	public void readWalkInfoAndSendOffsetFromOneKmer(Iterator<RayScaffoldingMessage> msgIterator){
		RayScaffoldingMessage incomingMsg;
		while(msgIterator.hasNext()){
			incomingMsg = msgIterator.next();
			if (incomingMsg.getComputeFlag()){
				outgoingMsg.reset();
				outgoingMsg.setComputeRulesFlag();
				outgoingMsg.setOffset(offset(incomingMsg.getKmer()));
				outgoingMsg.setIndex(incomingMsg.getIndex());
				outgoingMsg.setWalkSize(incomingMsg.getWalkSize());			
				sendMsg(incomingMsg.getKmer(), outgoingMsg);
			}
		}
		
	}
	
	public void sendMsgToWalkVertices(Iterator<RayScaffoldingMessage> msgIterator){
		//Don't like it!
		RayScaffoldingMessage incomingMsg;
		while(msgIterator.hasNext()){
			incomingMsg = msgIterator.next();	
			if(incomingMsg.getNeighborFlag()){ 
				outgoingMsg.reset();
				outgoingMsg.setKmer(getVertexId()) ;
				outgoingMsg.setComputeFlag();
				outgoingMsg.setWalkSize(incomingMsg.getWalkSize());
				for (VKmer vertexId : incomingMsg.getWalk()){
					outgoingMsg.setIndex(incomingMsg.getWalk().indexOf(vertexId));
					sendMsg(vertexId, outgoingMsg);
				}
			}
		}	
	}
	
	public void readOffsetInfoAndFindTheWholeOffset(Iterator<RayScaffoldingMessage> msgIterator){
		RayScaffoldingMessage incomingMsg;
		while(msgIterator.hasNext()){
			incomingMsg = msgIterator.next();
			if (incomingMsg.getComputeRulesFlag()){
				getVertexValue().walkSize = incomingMsg.getWalkSize();
				getVertexValue().index = incomingMsg.getIndex();
				rules_add(incomingMsg.getOffset());
				sendRuleValuestoLastVertex();
			}
		}	
	}
	
	public void sendRuleValuestoLastVertex(){
		outgoingMsg.reset();
		outgoingMsg.setRules(getVertexValue().rules_a, getVertexValue().rules_b, getVertexValue().rules_c);
		outgoingMsg.setKmer(getVertexId());
		sendMsg(getVertexValue().lastKmer, outgoingMsg);
	}
	
	public void chooseTheWinner(Iterator<RayScaffoldingMessage> msgIterator){
		
		//this is funny, do sth about it
		RayScaffoldingMessage incomingMsg;
		int ruleA = 0;
		int ruleB = 0;
		int ruleC = 0;
		VKmer winner = null; //humm?
		if(msgIterator.hasNext()){
			incomingMsg = msgIterator.next();
			winner = incomingMsg.getKmer();
			ruleA = incomingMsg.getRuleA();
			ruleB = incomingMsg.getRuleB();
			ruleC = incomingMsg.getRuleC();
		}		  
		while(msgIterator.hasNext()){
			double m = getM();
			incomingMsg = msgIterator.next();
			if ((m * ruleA < incomingMsg.getRuleA()) && (m * ruleB <  incomingMsg.getRuleB()) && (m * ruleC < incomingMsg.getRuleC())){
				winner = incomingMsg.getKmer();
				ruleA = incomingMsg.getRuleA();
				ruleB = incomingMsg.getRuleB();
				ruleC = incomingMsg.getRuleC();	
			}
		}
		outgoingMsg.reset();
		outgoingMsg.setWalk(getVertexValue().walk);
		sendMsg(winner, outgoingMsg);
		//return winner;
	}



	public double getM(){
		double m = 0;
		float coverage = getVertexValue().getAverageCoverage();
		if ((coverage>=2) && (coverage <= 19)){
			m = 3;
		} else if ((coverage>=20) && (coverage <= 24)){
			m = 2;
		} else if ((coverage>=25)){
			m = 1.3;
		}
		return m;
	}
	
	public void scaffold(Iterator<RayScaffoldingMessage> msgIterator){
		
		sendMsgToBranches(DIR.FORWARD, msgIterator);
		
		//Next Step you are that neighbor
		sendMsgToWalkVertices(msgIterator);
		
		//Now you are one of the vertices in the walk
		readWalkInfoAndSendOffsetFromOneKmer(msgIterator);
		
		//Now you are the neighbor again
		readOffsetInfoAndFindTheWholeOffset(msgIterator);
		
		//Now you are the last Vertex Again
		//You have the values to compare
		
			
	}
	

	@Override
	public void compute(Iterator<RayScaffoldingMessage> msgIterator) throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	public void scaffoldComplete(Iterator<RayScaffoldingMessage> msgIterator) {
		
		if (getVertexValue().getAverageCoverage() > SCAFFOLDING_VERTEX_MIN_COVERAGE){
			initVertex();
		}else{
			voteToHalt();
		}
		
	}

}
