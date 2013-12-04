package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.ReadHeadInfo;
import edu.uci.ics.genomix.data.types.ReadHeadSet;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.vertex.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.tipremove.TipRemoveVertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class RayVertex extends DeBruijnGraphCleanVertex<ScaffoldingVertexValueWritable, RayScaffoldingMessage> {
    public static int SCAFFOLDING_VERTEX_MIN_COVERAGE = 50;
    private static final Logger LOG = Logger.getLogger(TipRemoveVertex.class.getName());
    PrintWriter writer;

    public void writeOnFile() throws FileNotFoundException, UnsupportedEncodingException {
        String s = "/home/ubuntu/workspace/Results/" + getVertexValue().toString().split("\t")[5] + ".txt";
        writer = new PrintWriter(s, "UTF-8");
    }

    public void initVertex() {
        ///Complete this!
        if (kmerSize == -1) {
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
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
            getVertexValue().walk = new VKmerList();
        }

    }

    //We're finding the neighbors and send the walk to them
    public void sendMsgToBranches() {
        DIR direction;
        if (getVertexValue().flipFalg) {
            direction = DIR.REVERSE;
        } else {
            direction = DIR.FORWARD;
        }
        getVertexValue().walk.append(getVertexId());
        //if there is just one neighbor?
        for (EDGETYPE et : direction.edgeTypes()) {
            for (VKmer neighbor : getVertexValue().getEdges(et)) {
                sendMsgToNeighbor(neighbor, et);
                //I'm  not sure if we need to keep this edgetype
            }
        }
    }

    //tell neighbor about the walk
    public void sendMsgToNeighbor(VKmer neighbor, EDGETYPE et) {
        //This made problem!!!VVV
        //outgoingMsg.reset();
        outgoingMsg.setEdgeType(et);
        outgoingMsg.setWalk(getVertexValue().walk);
        outgoingMsg.setNeighborFlag();
        //keep the decision point because we need to come back to it
        outgoingMsg.setLastVertex(getVertexId());
        sendMsg(neighbor, outgoingMsg);

    }

    // for each Kmer in the walk and each BranchKmer finds the offset
    public int offset(VKmer neighbor) {
        int offset = 0;
        ReadHeadSet readIds;
        if (getVertexValue().flipFalg) {
            readIds = getVertexValue().getFlippedReadIds();
        } else {
            readIds = getVertexValue().getUnflippedReadIds();
        }
        for (ReadHeadInfo read : readIds) {
            if (checkedDistance(read, neighbor)) {
                offset++;
            }
        }
        return offset;
    }

    public boolean checkedDistance(ReadHeadInfo readHead, VKmer neighbor) {
        //if neighbor exist in read table
        //How to work with a read?
        //int neighborPositionOnRead = getVertexValue().walk.size() - getVertexValue().index  + 1;
        int neighborPositionOnRead = getVertexValue().walkSize - getVertexValue().index;
        if (indexOfKmerOnRead(readHead, neighbor) == neighborPositionOnRead) {
            return true;
        } else {
            return false;
        }
    }

    public int indexOfKmerOnRead(ReadHeadInfo readHead, VKmer kmer) {
        int index;
        String read = readHead.getThisReadSequence().toString();
        String stringKmer = kmer.toString();
        index = read.toLowerCase().indexOf(stringKmer.toLowerCase());
        return index;
    }

    //compute the rules , add new sentence in each step	
    public void rules_add(int offset) {
        //int l = walkSize + K - 1;

        getVertexValue().rules_a = getVertexValue().rules_a + (getVertexValue().walkSize - getVertexValue().index)
                * offset;
        getVertexValue().rules_b = getVertexValue().rules_b + offset;
        if ((offset < getVertexValue().rules_c) && (offset != 0)) {
            getVertexValue().rules_c = offset;
        }
    }

    public void prepareTheWinnerAndTheLoser(Iterator<RayScaffoldingMessage> msgIterator) {
        RayScaffoldingMessage incomingMsg;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if (incomingMsg.getStartFlag()) {
                getVertexValue().walk = incomingMsg.getWalk();
                getVertexValue().flipFalg = incomingMsg.getFlipFlag();
                getVertexValue().startFlag = incomingMsg.getStartFlag();
            } else if (incomingMsg.getRemoveEdgesFlag()) {
                getVertexValue().getEdges(incomingMsg.getEdgeType().mirror()).remove(incomingMsg.getKmer());
            }
        }
    }

    public void readWalkInfoAndSendOffsetFromOneKmer(Iterator<RayScaffoldingMessage> msgIterator) {
        RayScaffoldingMessage incomingMsg;
        //outgoingMsg.reset();
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if (incomingMsg.getComputeFlag()) {
                if (outgoingMsg == null) {
                    outgoingMsg = new RayScaffoldingMessage();
                } else {
                    outgoingMsg.reset();
                }
                outgoingMsg.setComputeRulesFlag();
                outgoingMsg.setEdgeType(incomingMsg.getEdgeType());
                outgoingMsg.setWalkSize(incomingMsg.getWalkSize());
                getVertexValue().walkSize = incomingMsg.getWalkSize();
                outgoingMsg.setOffset(offset(incomingMsg.getKmer()));
                outgoingMsg.setIndex(incomingMsg.getIndex());

                sendMsg(incomingMsg.getKmer(), outgoingMsg);
            }
        }

    }

    public void sendMsgToWalkVertices(Iterator<RayScaffoldingMessage> msgIterator) {
        //If previsited it's not sending any message out.
        if (!getVertexValue().previsitedFlag) {
            RayScaffoldingMessage incomingMsg;
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if (incomingMsg.getNeighborFlag()) {
                    if (outgoingMsg == null) {
                        outgoingMsg = new RayScaffoldingMessage();
                    } else {
                        outgoingMsg.reset();
                    }
                    outgoingMsg.setEdgeType(incomingMsg.getEdgeType());
                    outgoingMsg.setKmer(getVertexId());
                    outgoingMsg.setComputeFlag();
                    outgoingMsg.setWalkSize(incomingMsg.getWalk().size());
                    for (VKmer vertexId : incomingMsg.getWalk()) {
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

    public void readOffsetInfoAndFindTheWholeOffset(Iterator<RayScaffoldingMessage> msgIterator) {
        RayScaffoldingMessage incomingMsg;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if (incomingMsg.getComputeRulesFlag()) {
                getVertexValue().walkSize = incomingMsg.getWalkSize();
                getVertexValue().index = incomingMsg.getIndex();
                rules_add(incomingMsg.getOffset());
                sendRuleValuestoLastVertex(incomingMsg.getEdgeType());
            }
        }
    }

    public void sendRuleValuestoLastVertex(EDGETYPE et) {
        outgoingMsg.reset();
        outgoingMsg.setEdgeType(et);
        outgoingMsg.setRules(getVertexValue().rules_a, getVertexValue().rules_b, getVertexValue().rules_c);
        outgoingMsg.setKmer(getVertexId());
        sendMsg(getVertexValue().lastKmer, outgoingMsg);
    }

    public void chooseTheWinner(Iterator<RayScaffoldingMessage> msgIterator) {

        //this is funny, do sth about it
        RayScaffoldingMessage incomingMsg;
        int ruleA = 0;
        int ruleB = 0;
        int ruleC = 0;
        EDGETYPE edge = null;
        VKmer winner = null; //humm?
        if (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            winner = incomingMsg.getKmer();
            edge = incomingMsg.getEdgeType();
            ruleA = incomingMsg.getRuleA();
            ruleB = incomingMsg.getRuleB();
            ruleC = incomingMsg.getRuleC();
        }
        while (msgIterator.hasNext()) {
            double m = getM();
            incomingMsg = msgIterator.next();
            if ((m * ruleA < incomingMsg.getRuleA()) && (m * ruleB < incomingMsg.getRuleB())
                    && (m * ruleC < incomingMsg.getRuleC())) {
                sendRemoveEdgesMsgToLoser(winner, edge);
                //Is this the right way?
                getVertexValue().getEdges(incomingMsg.getEdgeType()).remove(incomingMsg.getKmer());
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
        if (ruleC == 0) {
            getVertexValue().doneFlag = true;

        } else {
            outgoingMsg.reset();
            if ((edge == EDGETYPE.RR) || (edge == EDGETYPE.FR)) {
                outgoingMsg.setFlipFlag();
            }
            outgoingMsg.setWalk(getVertexValue().walk);
            outgoingMsg.setStartFlag();
            sendMsg(winner, outgoingMsg);
            //return winner;
        }

    }

    public void sendRemoveEdgesMsgToLoser(VKmer loser, EDGETYPE et) {
        outgoingMsg.reset();
        outgoingMsg.setEdgeType(et);
        outgoingMsg.setKmer(getVertexId());
        outgoingMsg.setRemoveEdgesFlag();
        sendMsg(loser, outgoingMsg);
    }

    //Is it the only way?
    public void removeLoserEdges(Iterator<RayScaffoldingMessage> msgIterator) {
        RayScaffoldingMessage incomingMsg;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if (incomingMsg.getRemoveEdgesFlag()) {
                getVertexValue().getEdges(incomingMsg.getEdgeType().mirror()).remove(incomingMsg.getKmer());
            } //getVertexValue().processDelete(neighborToDeleteEdgetype, keyToDelete);
        }
    }

    public double getM() {
        double m = 0;
        float coverage = getVertexValue().getAverageCoverage();
        if ((coverage >= 2) && (coverage <= 19)) {
            m = 3;
        } else if ((coverage >= 20) && (coverage <= 24)) {
            m = 2;
        } else if ((coverage >= 25)) {
            m = 1.3;
        }
        return m;
    }

    public void scaffold(Iterator<RayScaffoldingMessage> msgIterator) throws Exception {
        if (getSuperstep() == 1) {
            initVertex();
            if ((getVertexValue().getAverageCoverage() > SCAFFOLDING_VERTEX_MIN_COVERAGE)) {
                if (!getVertexValue().doneFlag) {
                    sendMsgToBranches();
                }
            } else {
                voteToHalt();
            }
        } else if (getSuperstep() == 2) {
            writeOnFile();
            //Next Step you are that neighbor
            sendMsgToWalkVertices(msgIterator);
            writer.println(getVertexValue());
            writer.close();
        } else if (getSuperstep() == 3) {
            //Now you are one of the vertices in the walk
            readWalkInfoAndSendOffsetFromOneKmer(msgIterator);
        }
        // I'm uncommenting the next part
        //This is the main process
        // I need to change the == to something like %
        /*
        if (!getVertexValue().doneFlag){
        	if (getSuperstep() == 1) {
        		sendMsgToBranches();
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
        }*/

    }

    @Override
    public void compute(Iterator<RayScaffoldingMessage> msgIterator) throws Exception {
        // TODO Auto-generated method stub
        //writeOnFile();
        scaffold(msgIterator);
        voteToHalt();

    }

    public static PregelixJob getConfiguredJob(
            GenomixJobConf conf,
            Class<? extends DeBruijnGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass)
            throws IOException {
        PregelixJob job = DeBruijnGraphCleanVertex.getConfiguredJob(conf, vertexClass);
        job.setVertexInputFormatClass(NodeToScaffoldingVertexInputFormat.class);
        job.setVertexOutputFormatClass(ScaffoldingVertexToNodeOutputFormat.class);
        //        job.setGlobalAggregatorClass(ScaffoldingAggregator.class);
        return job;
    }

}
