package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.BasicBFSTraverseVertex.SEARCH_TYPE;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.ReadHeadInfo;
<<<<<<< HEAD
import edu.uci.ics.genomix.type.ReadHeadSet;
=======
>>>>>>> 9e811bd7dd411531254df89d8ef755146993e28b
import edu.uci.ics.genomix.type.VKmer;

public class RayVertex extends DeBruijnGraphCleanVertex<ScaffoldingVertexValueWritable, RayScaffoldingMessage>{
	public static int SCAFFOLDING_VERTEX_MIN_COVERAGE = -1;
	//public int K = 21;
	//private ArrayList<VKmer> neighbors;
	//private boolean startPoint;
	//private boolean preVisited;
	// VKmer lastKmerInWalk;

	public void initVertex(){	
		///Complete this!
		if (kmerSize == -1){
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
		}
		
        if (SCAFFOLDING_VERTEX_MIN_COVERAGE < 0){
            SCAFFOLDING_VERTEX_MIN_COVERAGE = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.SCAFFOLDING_VERTEX_MIN_COVERAGE));
        }
        
		if (outgoingMsg == null) {
            outgoingMsg = new RayScaffoldingMessage();
        } else {
            outgoingMsg.reset();
        }
        
    //=====================>>
        //StatisticsAggregator.preGlobalCounters.clear();   
        //counters.clear();
        //getVertexValue().getCounters().clear();
    //=====================>>	
	    if (getVertexValue().walk == null) {
	    	getVertexValue().walk = new ArrayList<VKmer>(); 
	    } else{
	    	getVertexValue().walk.clear();
	    }
	}

	//We're finding the neighbors and send the walk to them
<<<<<<< HEAD
	public void sendMsgToBranches() {
		DIR direction;
		if (getVertexValue().flipFalg){
			direction = DIR.REVERSE;
		} else {
			direction = DIR.FORWARD;
		}
=======
	public void sendMsgToBranches(DIR direction, Iterator<RayScaffoldingMessage> msgIterator) {
		///need a method for this:
		getVertexValue().previsitedFlag = true;
		readWalk(msgIterator);
		getVertexValue().walk.add(getVertexId());
>>>>>>> 9e811bd7dd411531254df89d8ef755146993e28b
		//if there is just one neighbor?
		for (EDGETYPE et : direction.edgeTypes()){
			for (VKmer neighbor : getVertexValue().getEdgeMap(et).keySet()) {
				sendMsgToNeighbor(neighbor, et);	
				//I'm  not sure if we need to keep this edgetype
			}
		}
	}
<<<<<<< HEAD
	

	
=======
>>>>>>> 9e811bd7dd411531254df89d8ef755146993e28b
	//tell neighbor about the walk
	public void sendMsgToNeighbor(VKmer neighbor, EDGETYPE et){
		outgoingMsg.setEdgeType(et);
		outgoingMsg.setWalk(getVertexValue().walk);
		outgoingMsg.setNeighborFlag();
		//outgoingMsg.
		//keep the decision point because we need to come back to it
		outgoingMsg.setLastVertex(getVertexId());
		sendMsg(neighbor ,outgoingMsg);
		
	}
	

	// for each Kmer in the walk and each BranchKmer finds the offset
	public int offset(VKmer neighbor){
		int offset = 0;
<<<<<<< HEAD
		ReadHeadSet readIds;
		if (getVertexValue().flipFalg){
			readIds = getVertexValue().getFlippedReadIds();
		} else {
			readIds = getVertexValue().getUnflippedReadIds();
		}
		for (ReadHeadInfo read : readIds){
=======
		for (ReadHeadInfo read : getVertexValue().getStartReads()){
>>>>>>> 9e811bd7dd411531254df89d8ef755146993e28b
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
<<<<<<< HEAD
		read.getOffset();
=======
>>>>>>> 9e811bd7dd411531254df89d8ef755146993e28b
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
	
	
<<<<<<< HEAD
	public void prepareTheWinnerAndTheLoser(Iterator<RayScaffoldingMessage> msgIterator){
		RayScaffoldingMessage incomingMsg;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if(incomingMsg.getStartFlag()){
            	getVertexValue().walk = incomingMsg.getWalk();
            	getVertexValue().flipFalg = incomingMsg.getFlipFlag();
            	getVertexValue().startFlag = incomingMsg.getStartFlag();
            }
            else if (incomingMsg.getRemoveEdgesFlag()){
            	getVertexValue().getEdgeMap(incomingMsg.getEdgeType().mirror()).remove(incomingMsg.getKmer());
            }
=======
	public void readWalk(Iterator<RayScaffoldingMessage> msgIterator){
		RayScaffoldingMessage incomingMsg;
        if (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            getVertexValue().walk = incomingMsg.getWalk();
>>>>>>> 9e811bd7dd411531254df89d8ef755146993e28b
        }
	}
	
	public void readWalkInfoAndSendOffsetFromOneKmer(Iterator<RayScaffoldingMessage> msgIterator){
		RayScaffoldingMessage incomingMsg;
		outgoingMsg.reset();
		while(msgIterator.hasNext()){
			incomingMsg = msgIterator.next();
			if (incomingMsg.getComputeFlag()){
				outgoingMsg.reset();
				outgoingMsg.setComputeRulesFlag();
				outgoingMsg.setEdgeType(incomingMsg.getEdgeType());
				outgoingMsg.setOffset(offset(incomingMsg.getKmer()));
				outgoingMsg.setIndex(incomingMsg.getIndex());
				outgoingMsg.setWalkSize(incomingMsg.getWalkSize());			
				sendMsg(incomingMsg.getKmer(), outgoingMsg);
			}
		}
		
	}
	
	public void sendMsgToWalkVertices(Iterator<RayScaffoldingMessage> msgIterator){
		//Don't like it!
		//And it's RC is not previsited
		//If previsited it's not sending any message out.
		if(!getVertexValue().previsitedFlag){
			RayScaffoldingMessage incomingMsg;
			while(msgIterator.hasNext()){
				incomingMsg = msgIterator.next();	
				if(incomingMsg.getNeighborFlag()){ 
					outgoingMsg.reset();
					//Sure?
					outgoingMsg.setEdgeType(incomingMsg.getEdgeType());
					outgoingMsg.setKmer(getVertexId()) ;
					outgoingMsg.setComputeFlag();
					outgoingMsg.setWalkSize(incomingMsg.getWalkSize());
					for (VKmer vertexId : incomingMsg.getWalk()){
						outgoingMsg.setIndex(incomingMsg.getWalk().indexOf(vertexId));
						sendMsg(vertexId, outgoingMsg);
					}
				}
			}
			//Now you are a visited neighbor, we don't need you anymore
			//Sure?
			//getVertexValue().previsitedFlag = true;
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
				sendRuleValuestoLastVertex(incomingMsg.getEdgeType());
			}
		}	
	}
	
	public void sendRuleValuestoLastVertex(EDGETYPE et){
		outgoingMsg.reset();
		outgoingMsg.setEdgeType(et);
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
		EDGETYPE edge = null;
		VKmer winner = null; //humm?
		if(msgIterator.hasNext()){
			incomingMsg = msgIterator.next();
			winner = incomingMsg.getKmer();
			edge = incomingMsg.getEdgeType();
			ruleA = incomingMsg.getRuleA();
			ruleB = incomingMsg.getRuleB();
			ruleC = incomingMsg.getRuleC();
		}		  
		while(msgIterator.hasNext()){
			double m = getM();
			incomingMsg = msgIterator.next();
			if ((m * ruleA < incomingMsg.getRuleA()) && (m * ruleB <  incomingMsg.getRuleB()) && (m * ruleC < incomingMsg.getRuleC())){
				sendRemoveEdgesMsgToLoser(winner, edge);
				//Is this the right way?
				getVertexValue().getEdgeMap(incomingMsg.getEdgeType()).remove(incomingMsg.getKmer());
				winner = incomingMsg.getKmer();
				ruleA = incomingMsg.getRuleA();
				ruleB = incomingMsg.getRuleB();
				ruleC = incomingMsg.getRuleC();	
				edge = incomingMsg.getEdgeType();
			} else {
				sendRemoveEdgesMsgToLoser(incomingMsg.getKmer(), incomingMsg.getEdgeType());
			}
		}
		// Do we really have a Winner?
		if(ruleC == 0){
			getVertexValue().doneFlag = true;
			
		} else {
			outgoingMsg.reset();
<<<<<<< HEAD
			if ((edge == EDGETYPE.RR) || (edge == EDGETYPE.FR)){
				outgoingMsg.setFlipFlag();
			}
			outgoingMsg.setWalk(getVertexValue().walk);
			outgoingMsg.setStartFlag();
=======
			outgoingMsg.setWalk(getVertexValue().walk);
>>>>>>> 9e811bd7dd411531254df89d8ef755146993e28b
			sendMsg(winner, outgoingMsg);
			//return winner;
		}
		
	}

	public void sendRemoveEdgesMsgToLoser(VKmer loser, EDGETYPE et){
		outgoingMsg.reset();
		outgoingMsg.setEdgeType(et);
		outgoingMsg.setKmer(getVertexId());
		outgoingMsg.setRemoveEdgesFlag();
		sendMsg(loser,outgoingMsg);
	}
	//Is it the only way?
	public void removeLoserEdges(Iterator<RayScaffoldingMessage> msgIterator){
		RayScaffoldingMessage incomingMsg;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if (incomingMsg.getRemoveEdgesFlag()){
            	getVertexValue().getEdgeMap(incomingMsg.getEdgeType().mirror()).remove(incomingMsg.getKmer());
            }	//getVertexValue().processDelete(neighborToDeleteEdgetype, keyToDelete);
        }
	}
<<<<<<< HEAD
		
=======
	
>>>>>>> 9e811bd7dd411531254df89d8ef755146993e28b
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
		
		if (getVertexValue().getAverageCoverage() > SCAFFOLDING_VERTEX_MIN_COVERAGE){
			initVertex();
		} else {
			voteToHalt();
		}
		if (!getVertexValue().doneFlag){
			if (getSuperstep() == 1) {
				sendMsgToBranches(DIR.FORWARD, msgIterator);
	        } else if (getSuperstep() == 2){
	        	//Next Step you are that neighbor
	    		sendMsgToWalkVertices(msgIterator);
	        } else if (getSuperstep() == 3){
	        	//Now you are one of the vertices in the walk
	    		readWalkInfoAndSendOffsetFromOneKmer(msgIterator);		
	        } else if (getSuperstep() == 4){
	        	//Now you are the neighbor again
	    		readOffsetInfoAndFindTheWholeOffset(msgIterator);
	        } else if (getSuperstep() == 5){
	        	//Now you are the last Vertex Again
	    		//You have the values to compare
	    		chooseTheWinner(msgIterator);
	        } else if (getSuperstep() == 5){
	        	//Again you are the neighbor and you need to remove those extra edges
	    		removeLoserEdges(msgIterator);	
	        }
		}
			
			
	}
	

	@Override
	public void compute(Iterator<RayScaffoldingMessage> msgIterator) throws Exception {
		// TODO Auto-generated method stub
	}
	

}
