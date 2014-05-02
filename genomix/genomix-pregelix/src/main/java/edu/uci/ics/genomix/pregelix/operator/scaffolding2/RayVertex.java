package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.security.SecureRandom;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.apache.commons.collections.SetUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.Node.NeighborInfo;
import edu.uci.ics.genomix.data.types.ReadHeadInfo;
import edu.uci.ics.genomix.data.types.ReadHeadSet;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.scaffolding2.RayMessage.RayMessageType;
import edu.uci.ics.genomix.pregelix.operator.scaffolding2.RayScores.Rules;
import edu.uci.ics.genomix.pregelix.operator.walkprocessor.WalkHandler;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class RayVertex extends DeBruijnGraphCleanVertex<RayValue, RayMessage> {
    private static DIR INITIAL_DIRECTION;
    private static HashSet<String> SEED_IDS;
    private Integer SEED_SCORE_THRESHOLD;
    private Integer SEED_LENGTH_THRESHOLD;
    private int COVERAGE_DIST_NORMAL_MEAN;
    private int COVERAGE_DIST_NORMAL_STD;
    private static boolean HAS_PAIRED_END_READS;
    private static int MAX_READ_LENGTH;
    private static int MAX_OUTER_DISTANCE;
    private static int MIN_OUTER_DISTANCE;
    private static int MAX_DISTANCE; // the max(readlengths, outerdistances)
    private static int EXPAND_CANDIDATE_BRANCHES_MAX_DISTANCE; // the max distance (in bp) the candidate branch expansion should reach across. 

    public static boolean REMOVE_OTHER_OUTGOING; // whether to remove other outgoing branches when a dominant edge is chosen
    public static boolean REMOVE_OTHER_INCOMING; // whether to remove other incoming branches when a dominant edge is chosen
    public static boolean CANDIDATES_SCORE_WALK; // whether to have the candidates score the walk
    public static boolean EXPAND_CANDIDATE_BRANCHES; // whether to get kmer from all possible candidate branches
    public static boolean DELAY_PRUNE; // Whether we should perform the prune as a separate job, after all the walks have completed their march.
    public static boolean EARLY_STOP;	// whether to stop early if there is large overlap with other walks
    public static boolean SAVE_BEST_PATH;
    
    public static final boolean STORE_WALK = false; // whether to save the walks - just for test 
    private static int OVERLAP_THRESHOLD = 500;
    
    PrintWriter writer;
    PrintWriter log;
    
    public void writeOnFile(String Name) throws FileNotFoundException, UnsupportedEncodingException {
        String s = "/home/elmira/WORK/RESULTS/files/" + Name.toString() + ".txt";
        try {
			writer = new PrintWriter(new BufferedWriter(new FileWriter(s, true)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    
    public RayVertex() {
        outgoingMsg = new RayMessage();
    }

    @Override
    public void configure(Configuration conf) {
        super.configure(conf);
        initVertex();
        // TODO maybe have FORWARD and REVERSE happening at the same time?
        INITIAL_DIRECTION = DIR.valueOf(conf.get(GenomixJobConf.SCAFFOLDING_INITIAL_DIRECTION));
        COVERAGE_DIST_NORMAL_MEAN = 0; // TODO set properly once merged
        COVERAGE_DIST_NORMAL_STD = 1000;

        if (conf.get(GenomixJobConf.SCAFFOLD_SEED_IDS) != null) {
            SEED_IDS = new HashSet<>(Arrays.asList(conf.get(GenomixJobConf.SCAFFOLD_SEED_IDS).split(",")));
        } else {
            try {
                SEED_SCORE_THRESHOLD = Integer.parseInt(conf.get(GenomixJobConf.SCAFFOLDING_SEED_SCORE_THRESHOLD));
            } catch (NumberFormatException e) {
                SEED_LENGTH_THRESHOLD = Integer.parseInt(conf.get(GenomixJobConf.SCAFFOLDING_SEED_LENGTH_THRESHOLD));
            }
        }

        HAS_PAIRED_END_READS = GenomixJobConf.outerDistanceMeans != null;
        MAX_READ_LENGTH = Integer.MIN_VALUE;
        MAX_OUTER_DISTANCE = Integer.MIN_VALUE;
        MIN_OUTER_DISTANCE = Integer.MAX_VALUE;
        for (byte libraryId = 0; libraryId < GenomixJobConf.readLengths.size(); libraryId++) {
            MAX_READ_LENGTH = Math.max(MAX_READ_LENGTH, GenomixJobConf.readLengths.get(libraryId));
            if (HAS_PAIRED_END_READS && GenomixJobConf.outerDistanceMeans.containsKey(libraryId)) {
                MAX_OUTER_DISTANCE = Math.max(MAX_OUTER_DISTANCE, GenomixJobConf.outerDistanceMeans.get(libraryId)
                        + GenomixJobConf.outerDistanceStdDevs.get(libraryId));
                MIN_OUTER_DISTANCE = Math.min(MIN_OUTER_DISTANCE, GenomixJobConf.outerDistanceMeans.get(libraryId)
                        - GenomixJobConf.outerDistanceStdDevs.get(libraryId));
            }
        }
        MAX_DISTANCE = Math.max(MAX_OUTER_DISTANCE, MAX_READ_LENGTH);
        EXPAND_CANDIDATE_BRANCHES_MAX_DISTANCE = Math.min(MAX_DISTANCE, conf.getInt(GenomixJobConf.SCAFFOLDING_EXPAND_CANDIDATE_BRANCHES_MAX_DISTANCE, Integer.MAX_VALUE));
        
        REMOVE_OTHER_OUTGOING = Boolean.parseBoolean(conf.get(GenomixJobConf.SCAFFOLDING_REMOVE_OTHER_OUTGOING));
        REMOVE_OTHER_INCOMING = Boolean.parseBoolean(conf.get(GenomixJobConf.SCAFFOLDING_REMOVE_OTHER_INCOMING));
        CANDIDATES_SCORE_WALK = Boolean.parseBoolean(conf.get(GenomixJobConf.SCAFFOLDING_CANDIDATES_SCORE_WALK));
        EXPAND_CANDIDATE_BRANCHES = Boolean.parseBoolean(conf.get(GenomixJobConf.SCAFFOLDING_EXPAND_CANDIDATE_BRANCHES));
        DELAY_PRUNE = Boolean.parseBoolean(conf.get(GenomixJobConf.SCAFFOLDING_DELAY_PRUNE));
        EARLY_STOP = Boolean.parseBoolean(conf.get(GenomixJobConf.SCAFFOLDING_EARLY_STOP));
        SAVE_BEST_PATH = Boolean.parseBoolean(conf.get(GenomixJobConf.SCAFFOLDING_SAVE_BEST_PATH));

        if (getSuperstep() == 1) {
            // manually clear state
            //getVertexValue().visited = false;
        	getVertexValue().getIntersection().put(getVertexId(), false);
            // FIXME
            if (INITIAL_DIRECTION == DIR.REVERSE){
            	getVertexValue().getFlippedFromInitDir().put(getVertexId(),true);
            }else {
            	getVertexValue().getFlippedFromInitDir().put(getVertexId(),false);
            }
            getVertexValue().getStopSearch().put(getVertexId(), false);
        }
    }

    @Override
    public void compute(Iterator<RayMessage> msgIterator) throws Exception {
        if (getSuperstep() == 1) {
            if (isStartSeed()) {
        	//if (getVertexId().toString().equals("CAAAAAGAAAAAACCCGCCGC") || getVertexId().toString().equals("ATAAGACGCGCCAGCGTCGCA") ){
                msgIterator = getStartMessage();
                LOG.info("starting seed in " + INITIAL_DIRECTION + ": " + getVertexId() + ", length: "
                        + getVertexValue().getKmerLength() + ", coverge: " + getVertexValue().getAverageCoverage()
                        + ", score: " + getVertexValue().calculateSeedScore());
            }
        }
        
        scaffold(msgIterator);
        voteToHalt();
    }

    /**
     * @return whether or not this node meets the "seed" criteria
     */
    private boolean isStartSeed() {
    	if (getVertexValue().degree(INITIAL_DIRECTION) == 0) {
    		return false;
    	}
        if (SEED_IDS != null) {
            return SEED_IDS.contains(getVertexId().toString());
        } else {
            if (SEED_SCORE_THRESHOLD != null) {
                float coverage = getVertexValue().getAverageCoverage();
                return ((coverage >= COVERAGE_DIST_NORMAL_MEAN - COVERAGE_DIST_NORMAL_STD)
                        && (coverage <= COVERAGE_DIST_NORMAL_MEAN + COVERAGE_DIST_NORMAL_STD) && (getVertexValue()
                        .calculateSeedScore() >= SEED_SCORE_THRESHOLD));
            } else {
                return getVertexValue().getKmerLength() >= SEED_LENGTH_THRESHOLD;
            }
        }
    }
    /**
     * @return an iterator with a single CONTINUE_WALK message including this node only.
     *         the edgeTypeBackToFrontier should be the flip of INITIAL_DIRECTION
     *         this allows us to call the `scaffold` function the same way in all iterations.
     */
    private Iterator<RayMessage> getStartMessage() {
        RayMessage initialMsg = new RayMessage();
        initialMsg.setMessageType(RayMessageType.CONTINUE_WALK);
        initialMsg.setWalkLength(0);
        initialMsg.setSeed(getVertexId());
        if (INITIAL_DIRECTION == DIR.FORWARD) {
            initialMsg.setFrontierFlipped(false);
            initialMsg.setEdgeTypeBackToFrontier(EDGETYPE.RR);
        } else {
            initialMsg.setFrontierFlipped(true);
            initialMsg.setEdgeTypeBackToFrontier(EDGETYPE.FF);
            //FIXME
            initialMsg.setCandidateFlipped(true);
            /**
            try {
				initialMsg.setWalkIds(loadWalkMap(directory));
				initialMsg.setAccumulatedWalkKmer(loadAccWalk(directory));
				initialMsg.setSeed(getVertexId());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			**/
        }
        return Collections.singletonList(initialMsg).iterator();
    }

    // local variables for scaffold
    private HashMap<VKmer, ArrayList<RayMessage>> requestScoreMsgs = new HashMap<>();
    private HashMap<VKmer, ArrayList<RayMessage>> aggregateScoreMsgs = new HashMap<>();

    private void scaffold(Iterator<RayMessage> msgIterator) throws IOException {
        // TODO since the messages aren't synchronized by iteration, we might want to  
        // manually order our messages to make the process a little more deterministic
        // for example, one node could receive both a "continue" message and a "request kmer"
        // message in the same iteration.  Who should win? the result will be an intersection 
        // in either case but should the "continue" msg always be allowed to be processed and
        // a "stop" always go back towards the "request kmer" walk?
        VKmer id = getVertexId();
        RayValue vertex = getVertexValue();
        requestScoreMsgs.clear();
        aggregateScoreMsgs.clear();
        while (msgIterator.hasNext()) {
            RayMessage msg = msgIterator.next();
            switch (msg.getMessageType()) {
                case CONTINUE_WALK:
                	if (getSuperstep() < maxIteration) {
                		startBranchComparison(msg);
                	}
                    break;
                case REQUEST_CANDIDATE_KMER:
                    sendCandidatesToFrontier(msg);
                    break;
                case ASSEMBLE_CANDIDATES:
                	//boolean repeat = false;
                	if(vertex.getCandidateMsgsMap().containsKey(msg.getSeed())){
                		vertex.getCandidateMsgsMap().get(msg.getSeed()).add(msg);
                		
                	}else{
                		ArrayList<RayMessage> msgs = new ArrayList<>();
                		msgs.add(msg);
                		vertex.getCandidateMsgsMap().put(msg.getSeed(), msgs);
                	}
                	int temp = vertex.getPendingCandiateBranchesMap().get(msg.getSeed());
            		HashMap <VKmer, Integer> tempMap = vertex.getPendingCandiateBranchesMap();
            		tempMap.put(new VKmer(msg.getSeed()), temp - 1);
            		vertex.setPendingCandidateBranchesMap(new HashMap(tempMap));
//            		LOG.info("recieved complete candidate. total pending searches:" + vertex.pendingCandidateBranchesMap.size());

                    break;
                case REQUEST_SCORE:
                    // batch-process these (have to truncate to min length)
                	if (requestScoreMsgs.containsKey(msg.getSeed())){
                     requestScoreMsgs.get(msg.getSeed()).add(new RayMessage(msg));
                	}else{
                		requestScoreMsgs.put(new VKmer(msg.getSeed()), new ArrayList<>(Collections.singletonList(new RayMessage(msg))));
                     }
                	
                    break;
                case AGGREGATE_SCORE:
                	if (aggregateScoreMsgs.containsKey(msg.getSeed())){
                		aggregateScoreMsgs.get(msg.getSeed()).add(new RayMessage(msg));
                   	}else{
                   		aggregateScoreMsgs.put(new VKmer(msg.getSeed()), new ArrayList<>(Collections.singletonList(new RayMessage(msg))));
                        }
                	
                    break;
                case PRUNE_EDGE:
                    // remove the given edge back towards the frontier node.  Also, clear the "visited" state of this node
                    // so that other walks are free to include me
                    try {
                        vertex.getEdges(msg.getEdgeTypeBackToFrontier()).remove(msg.getSourceVertexId());
                    } catch (ArrayIndexOutOfBoundsException e) {
                        LOG.severe("Was asked to prune an edge that doesn't exist here! I am " + id + " = " + vertex
                                + " and recieved msg " + msg.getMessageType() + " " + msg.getEdgeTypeBackToFrontier()
                                + " " + msg.getSourceVertexId());
                    }
                    //vertex.visited = false;
                    List<VKmer> tmp = vertex.getVisitedList();
                    tmp.remove(msg.getSeed());
                    vertex.setVisitedList(new ArrayList(tmp));
                    break;
                case ADD_EDGE:
                	// TODO add a delay option here?
                	if (!vertex.getEdges(msg.getEdgeTypeBackToFrontier()).contains(msg.getSourceVertexId())) {
                		vertex.getEdges(msg.getEdgeTypeBackToFrontier()).append(msg.getSourceVertexId());
                	}
                	break;
                case STOP:
                    // I am a frontier node but one of my neighbors was already included in a different walk.
                    // that neighbor is marked "intersection" and I need to remember the stopped state.  No path
                    // can continue out of me in the future, even when I receive aggregate scores
                    vertex.getStopSearch().put(msg.getSeed() ,true);
                    break;
                case UPDATE_FORK_COUNT:
                	int preNumberOfForks = vertex.getPendingCandiateBranchesMap().get(msg.getSeed());
                    vertex.getPendingCandiateBranchesMap().put(msg.getSeed(), preNumberOfForks + msg.getNumberOfForks());
                    LOG.info("new candidate registered. total pending searches for " + vertex.getPendingCandiateBranchesMap().size() + "seeds");
                    break;
            }
        }
        if (requestScoreMsgs.size() > 0) {
        	for (VKmer seed : requestScoreMsgs.keySet()){
        		sendScoresToFrontier(requestScoreMsgs.get(seed));
        	}
        }
        if (aggregateScoreMsgs.size() > 0) {
        	for (VKmer seed : aggregateScoreMsgs.keySet()){
        		compareScoresAndPrune(aggregateScoreMsgs.get(seed));
        	}
        }
        /**
        if (vertex.pendingCandidateBranches != null && vertex.pendingCandidateBranches == 0
                && vertex.getCandidateMsgs().size() > 0) {
            sendCandidateKmersToWalkNodes(vertex.getCandidateMsgs());
            vertex.getCandidateMsgs().clear();
            vertex.pendingCandidateBranches =vertex.pendingCandidateBranchesCopy;
        }
        **/
        //not a good condition
        if (vertex.getCandidateMsgsMap().size() > 0){
        	ArrayList<VKmer> visited = new ArrayList<>();
        	for (Entry <VKmer, Integer> entry : vertex.getPendingCandiateBranchesMap().entrySet()){
        		if (entry.getValue() == 0){       				
            		sendCandidateKmersToWalkNodes(vertex.getCandidateMsgsMap().get(entry.getKey()));
            		visited.add(entry.getKey());
        		}
        	}
        	
        	for (VKmer seed : visited){
        		vertex.getPendingCandiateBranchesMap().remove(seed);
        	}
        	if (vertex.getPendingCandiateBranchesMap().size() == 0) {
        		vertex.getCandidateMsgsMap().clear();
        	}
        }
        
    }

    private void sendCandidateKmersToWalkNodes(ArrayList<RayMessage> candidateMsgs) {
//        LOG.info("sending " + candidateMsgs.size() + " candidates to " + candidateMsgs.get(0).getWalkIds().size()
//                + " walk nodes");
        for (int candIndex = 0; candIndex < candidateMsgs.size(); candIndex++) {
            RayMessage msg = candidateMsgs.get(candIndex);
            for (int walkIndex = 0; walkIndex < msg.getWalkIds().size(); walkIndex++) {
                outgoingMsg.setAsCopy(msg);
                outgoingMsg.setMessageType(RayMessageType.REQUEST_SCORE);
                outgoingMsg.getSeed().reset(kmerSize);
                outgoingMsg.setSeed(new VKmer(msg.getSeed()));
                outgoingMsg.getWalkOffsets().clear();
                outgoingMsg.getWalkOffsets().add(msg.getWalkOffsets().get(walkIndex)); // the offset of the destination node
                outgoingMsg.getWalkIds().clear();
                outgoingMsg.getWalkIds().append(new VKmer(msg.getWalkIds().getPosition(walkIndex)));
                outgoingMsg.setPathIndex(candIndex);
                sendMsg(msg.getWalkIds().getPosition(walkIndex), outgoingMsg);
            }
        }
    }

    private void startBranchComparison(RayMessage msg) throws FileNotFoundException, UnsupportedEncodingException {
        // I'm the new frontier node... time to do some pruning
        VKmer id = getVertexId();
        RayValue vertex = getVertexValue();
        VKmer realSeed = msg.getSeed();
        
        // must explicitly check if this node was visited already (would have happened in 
        // this iteration as a previously processed msg!)
        if(msg.getWalkLength() > 0){
        	if(realSeed.equals(id)){
        		if (STORE_WALK){
        			storeWalk(msg.getWalkIds(),msg.getAccumulatedWalkKmer(),realSeed);
        		}
        		vertex.getIntersection().put(realSeed, true);
           		vertex.getStopSearch().put(realSeed, true);
           		LOG.info("start branch comparison had to stop at " + id + " with total length: " + msg.getWalkLength()
           				+ "\n>id " + id + "\n" + msg.getAccumulatedWalkKmer());
           		return;
        	}

        } 
        if (!vertex.getVisitedList().contains(realSeed)){
        	vertex.getVisitedList().add(realSeed);
        } else {
        	if (STORE_WALK){
        		storeWalk(msg.getWalkIds(),msg.getAccumulatedWalkKmer(), realSeed);
        	}
            LOG.info("reached dead end as a cycle at " + id + " with total length: " + msg.getWalkLength() + "\n>id " + id + "\n"
                    + msg.getAccumulatedWalkKmer());
            //vertex.stopSearch = true;
            return;
        }

        for (VKmer visitedSeed: vertex.getVisitedList()){
        	if (!msg.getVisitCounter().containsKey(visitedSeed)){
        		msg.getVisitCounter().put(visitedSeed, getVertexValue().getKmerLength() - kmerSize + 1);
        	}
        	else{
        		int overlapLength = msg.getVisitCounter().get(visitedSeed) + getVertexValue().getKmerLength() - kmerSize + 1;
        		msg.getVisitCounter().put(visitedSeed, overlapLength);
        	}
        }
        
        if(EARLY_STOP){
			LOG.info("overlap check for walk ending at" + id + " with total length: " + msg.getWalkLength());
        	for (Entry<VKmer, Integer> entry : msg.getVisitCounter().entrySet()){
        		if (entry.getValue()> OVERLAP_THRESHOLD && !(entry.getKey().equals(realSeed))){
        			LOG.info("early stop for walk ending at" + id + "with total length: " + msg.getWalkLength() + "\n" + msg.getAccumulatedWalkKmer());
        			if (STORE_WALK){
        				storeWalk(msg.getWalkIds(),msg.getAccumulatedWalkKmer(), realSeed);

        			}
        			return;
        		}
        	}
        }
        
        // I am the new frontier but this message is coming from the previous frontier; I was the "candidate"
        vertex.getFlippedFromInitDir().put(realSeed, msg.getCandidateFlipped());
        DIR nextDir = msg.getEdgeTypeBackToFrontier().mirror().neighborDir();
        
        if (REMOVE_OTHER_INCOMING && getSuperstep() > 1) {
            removeOtherIncomingEdges(msg, id, vertex);
        }

        msg.visitNode(id, vertex, INITIAL_DIRECTION, realSeed);
        
        
        if (vertex.degree(nextDir) == 0) {
            // this walk has reached a dead end!  nothing to do in this case.
        	if (STORE_WALK){
        		storeWalk(msg.getWalkIds(),msg.getAccumulatedWalkKmer(), realSeed);
        	}
        	LOG.info("reached dead end at " + id + " with total length: " + msg.getWalkLength() + "\n>id " + id + "\n"
                    + msg.getAccumulatedWalkKmer());
            //vertex.stopSearch = true;
            return;
        } else if (vertex.degree(nextDir) == 1) {
            // one neighbor -> just send him a continue msg w/ me added to the list
            NeighborInfo next = vertex.getSingleNeighbor(nextDir);
            msg.setEdgeTypeBackToFrontier(next.et.mirror());
            msg.setFrontierFlipped(vertex.getFlippedFromInitDir().get(realSeed));
            msg.setCandidateFlipped(vertex.getFlippedFromInitDir().get(realSeed) ^ next.et.mirror().causesFlip());
            msg.setSeed(new VKmer(realSeed));
            sendMsg(next.kmer, msg);
            LOG.info("bouncing over path node: " + id + ", accumulatedWalkKmer has length: " + msg.getAccumulatedWalkKmer().getKmerLetterLength());
        } else {
            // 2+ neighbors -> start evaluating candidates via a REQUEST_KMER msg
            msg.setMessageType(RayMessageType.REQUEST_CANDIDATE_KMER);
            msg.setFrontierFlipped(vertex.getFlippedFromInitDir().get(realSeed));
            msg.setSourceVertexId(new VKmer(id));
            msg.setSeed(new VKmer(realSeed));
            for (EDGETYPE et : nextDir.edgeTypes()) {
                for (VKmer next : vertex.getEdges(et)) {
                    msg.setEdgeTypeBackToFrontier(et.mirror());
                    msg.setEdgeTypeBackToPrev(et.mirror());
                    msg.setCandidateFlipped(vertex.getFlippedFromInitDir().get(realSeed) ^ et.mirror().causesFlip());
                    sendMsg(next, msg);
                    LOG.info("evaluating branch: " + et + ":" + next);
                }
            }

            // remember how many total candidate branches to expect
            vertex.getPendingCandiateBranchesMap().put(realSeed, vertex.degree(nextDir));
         
        }
    }

    private void removeOtherIncomingEdges(RayMessage msg, VKmer id, RayValue vertex) {
        // remove all other "incoming" nodes except the walk we just came from. on first iteration, we haven't really "hopped" from anywhere 
        // find the most recent node in the walk-- this was the frontier last iteration
        int lastIndex = getLastNodeIndex(msg);
        VKmer lastId = msg.getWalkIds().getPosition(lastIndex);
        DIR prevDir = msg.getEdgeTypeBackToFrontier().dir();

        if (DELAY_PRUNE || SAVE_BEST_PATH) {
        	Entry<EDGETYPE, VKmer> edges = new SimpleEntry<EDGETYPE, VKmer>(msg.getEdgeTypeBackToFrontier(), lastId);
        	LOG.info("incoming edge I'm saving: " + edges + "=" + msg.previousRules);
        	if (vertex.getIncomingEdgesToKeep().containsKey(edges)) {
        		if (vertex.getIncomingEdgesToKeep().get(edges) == null || (msg.previousRules != null && vertex.getIncomingEdgesToKeep().get(edges).ruleC < msg.previousRules.ruleC)) {
        			vertex.getIncomingEdgesToKeep().put(edges, msg.previousRules);
        		}
        	} else {
        		vertex.getIncomingEdgesToKeep().put(edges, msg.previousRules);
        	}
		} else {
	        for (EDGETYPE et : prevDir.edgeTypes()) {
	            Iterator<VKmer> it = vertex.getEdges(et).iterator();
	            while (it.hasNext()) {
	                VKmer other = it.next();
	                if (et != msg.getEdgeTypeBackToFrontier() || !other.equals(lastId)) {
	                    // only keep the dominant edge
	                    outgoingMsg.reset();
	                    outgoingMsg.setMessageType(RayMessageType.PRUNE_EDGE);
	                    outgoingMsg.setEdgeTypeBackToFrontier(et.mirror());
	                    outgoingMsg.setSourceVertexId(new VKmer(id));
	                    sendMsg(other, outgoingMsg);
	                    it.remove();
	                    //                        vertex.getEdges(et).remove(other);
	                }
	            }
	        }
        }
    }


	private static int getLastNodeIndex(RayMessage msg) {
		int lastOffset = Integer.MIN_VALUE;
        int lastIndex = -1;
        for (int i = 0; i < msg.getWalkIds().size(); i++) {
            if (msg.getWalkOffsets().get(i) > lastOffset) {
                lastOffset = msg.getWalkOffsets().get(i);
                lastIndex = i;
            }
        }
		return lastIndex;
	}

    // local variables for sendCandidatesToFrontier
    private transient RayMessage tmpCandidate = new RayMessage();

    /**
     * I'm a candidate and need to send my kmer to all the nodes of the walk
     * First, the candidate kmer needs to be aggregated
     * if I've been visited already, I need to send a STOP msg back to the frontier node and
     * mark this node as an "intersection" between multiple seeds
     * 
     * @param msg
     * @throws UnsupportedEncodingException 
     * @throws FileNotFoundException 
     */
    private void sendCandidatesToFrontier(RayMessage msg) throws FileNotFoundException, UnsupportedEncodingException {
        VKmer id = getVertexId();
        RayValue vertex = getVertexValue();
        VKmer seed = new VKmer();
        DIR nextDir = msg.getEdgeTypeBackToPrev().mirror().neighborDir();
        // already visited -> the frontier must stop!
        seed = msg.getSeed();
        if ((vertex.getVisitedList()!=null) && ((vertex.getVisitedList().contains(seed)))) {
        	if (STORE_WALK){
        		storeWalk(msg.getWalkIds(),msg.getAccumulatedWalkKmer(), seed);
        	}
            vertex.getIntersection().put(seed, true);
            //vertex.stopSearch = true;
            outgoingMsg.reset();
            outgoingMsg.setSeed(new VKmer(seed));
            outgoingMsg.setMessageType(RayMessageType.STOP);
            sendMsg(msg.getSourceVertexId(), outgoingMsg);
            return;
        }

        ArrayList<RayScores> singleEndScores = null;
        ArrayList<RayScores> pairedEndScores = null;
        if (CANDIDATES_SCORE_WALK) {
            // this candidate node needs to score the accumulated walk
            vertex.getFlippedFromInitDir().put(seed, msg.getCandidateFlipped());
            tmpCandidate.reset();
            // pretend that the candidate is the frontier and the frontier is the candidate
            tmpCandidate.setFrontierFlipped(vertex.getFlippedFromInitDir().get(seed));
            tmpCandidate.setEdgeTypeBackToFrontier(msg.getEdgeTypeBackToFrontier());
            tmpCandidate.setToScoreKmer(new VKmer(msg.getAccumulatedWalkKmer().reverse()));
            tmpCandidate.setToScoreId(new VKmer(id));
            // TODO check this... the accumulated kmer is always in the same dir as the search, right?
            // So, when we're going backward it's always flipped?
            tmpCandidate.setCandidateFlipped(true); 
            
            // TODO need to truncate to only a subset of readids if the MAX_DISTANCE isn't being used for max candidate search length
            // TODO need to scale the ruleA back a bit-- the paths shouldn't have as much effect.
            singleEndScores = voteFromReads(true, vertex, !vertex.getFlippedFromInitDir().get(seed),
                    Collections.singletonList(tmpCandidate), 0, msg.getToScoreKmer().getKmerLetterLength() + vertex.getKmerLength() - kmerSize + 1, msg.getAccumulatedWalkKmer()
                            .getKmerLetterLength() - kmerSize + 1);
            pairedEndScores = HAS_PAIRED_END_READS ? voteFromReads(false, vertex, !vertex.getFlippedFromInitDir().get(seed),
                    Collections.singletonList(tmpCandidate), 0, msg.getToScoreKmer().getKmerLetterLength() + vertex.getKmerLength() - kmerSize + 1 , msg.getAccumulatedWalkKmer()
                            .getKmerLetterLength() - kmerSize + 1) : null;
            
            // TODO keep these scores?

            vertex.getFlippedFromInitDir().remove(seed);
        }

        outgoingMsg.reset();
        outgoingMsg.setSingleEndScores(new ArrayList<>(msg.getSingleEndScores()));
        if (singleEndScores != null) {
            outgoingMsg.getSingleEndScores().addAll(singleEndScores);
        }
        outgoingMsg.setPairedEndScores(new ArrayList<>(msg.getPairedEndScores()));
        if (pairedEndScores != null) {
            outgoingMsg.getPairedEndScores().addAll(pairedEndScores);
        }
        // keep previous msg details
        outgoingMsg.setSeed(new VKmer(seed));
        outgoingMsg.setFrontierFlipped(msg.getFrontierFlipped());
        outgoingMsg.setEdgeTypeBackToFrontier(msg.getEdgeTypeBackToFrontier());
        outgoingMsg.setSourceVertexId(new VKmer(msg.getSourceVertexId())); // frontier node
        outgoingMsg.setWalkLength(msg.getWalkLength());
        outgoingMsg.setAccumulatedWalkKmer(new VKmer(msg.getAccumulatedWalkKmer()));
        outgoingMsg.setWalkIds(new VKmerList(msg.getWalkIds()));
        outgoingMsg.setWalkOffsets(msg.getWalkOffsets());
        outgoingMsg.setVisitCounter(new HashMap(msg.getVisitCounter()));

        // get kmer and id to score in walk nodes
        boolean readyToScore = false;
        if (EXPAND_CANDIDATE_BRANCHES) {
            // need to add this vertex to the candidate walk then check if we've looked far enough.
            // also notify the frontier of additional branches
            if (msg.getToScoreId().getKmerLetterLength() == 0) {
                outgoingMsg.setToScoreId(new VKmer(id));
                outgoingMsg.setToScoreKmer(!msg.getCandidateFlipped() ? new VKmer(vertex.getInternalKmer()) : new VKmer(vertex
                        .getInternalKmer().reverse()));
            } else {
                outgoingMsg.setToScoreId(new VKmer(msg.getToScoreId()));
                outgoingMsg.setToScoreKmer(new VKmer(msg.getToScoreKmer()));
                EDGETYPE accumulatedCandidateToVertexET = !msg.getCandidateFlipped() ? EDGETYPE.FF : EDGETYPE.FR;
                outgoingMsg.getToScoreKmer().mergeWithKmerInDir(accumulatedCandidateToVertexET, Kmer.getKmerLength(),
                        vertex.getInternalKmer());
            }
            outgoingMsg.candidatePathIds.setAsCopy(msg.candidatePathIds);
            outgoingMsg.candidatePathIds.append(new VKmer(id));

            // pass this kmer along to my adjacent branches
            if (outgoingMsg.getToScoreKmer().getKmerLetterLength() >= EXPAND_CANDIDATE_BRANCHES_MAX_DISTANCE || vertex.degree(nextDir) == 0) {
                // this branch doesn't need to search any longer-- pass back to frontier
                readyToScore = true;
            }
        } else {
            outgoingMsg.setToScoreId(new VKmer(id)); // candidate node (me)
            outgoingMsg.setToScoreKmer(!msg.getCandidateFlipped() ? new VKmer(vertex.getInternalKmer()) : new VKmer(vertex.getInternalKmer()
                    .reverse()));
            readyToScore = true;
        }

        if (readyToScore) {
            // send this complete candidate to the frontier node
            outgoingMsg.setMessageType(RayMessageType.ASSEMBLE_CANDIDATES);
            outgoingMsg.setSeed(new VKmer(seed));
            sendMsg(new VKmer(msg.getSourceVertexId()), outgoingMsg);
            LOG.info("ready to score kmer " + outgoingMsg.getToScoreKmer() + " of candidate-length: "
                    + outgoingMsg.getToScoreKmer().getKmerLetterLength() + " for candidate "
                    + outgoingMsg.getEdgeTypeBackToFrontier().mirror() + ":" + outgoingMsg.getToScoreId()
                    + " which passed through " + outgoingMsg.candidatePathIds);
        } else {
            // candidate is incomplete; need info from neighbors
            outgoingMsg.setMessageType(RayMessageType.REQUEST_CANDIDATE_KMER);
            for (EDGETYPE et : nextDir.edgeTypes()) {
                for (VKmer neighbor : vertex.getEdges(et)) {
                    outgoingMsg.setEdgeTypeBackToPrev(et.mirror());
                    outgoingMsg.setSeed(new VKmer(seed));
                    outgoingMsg.setCandidateFlipped(msg.getCandidateFlipped() ^ et.causesFlip());
                    sendMsg(neighbor, outgoingMsg);
                }
            }
            
            // notify the frontier about the number of forks we generated (so they know when to stop waiting)
            if (vertex.degree(nextDir) > 1) {
                outgoingMsg.reset();
                outgoingMsg.setSeed(new VKmer(seed));
                outgoingMsg.setMessageType(RayMessageType.UPDATE_FORK_COUNT);
                outgoingMsg.setNumberOfForks(vertex.degree(nextDir) - 1);
                sendMsg(new VKmer(msg.getSourceVertexId()), outgoingMsg);
                LOG.info("forking " + (vertex.degree(nextDir) - 1) + " more branches");
            } else {
                LOG.info("getting candidate kmer part from single neighbor");
            }
        }
    }

    /**
     * I'm a node along the walk and am receiving a kmer that might be in my reads
     * I should be receiving multiple kmers (from all branches adjacent to the frontier)
     * I need to score the kmers but should truncate the kmers so they're all the same length before scoring.
     * after scoring, I should send the scores to the frontier node.
     * if I don't have any reads that are relevant (the walk has progressed beyond me),
     * then I shouldn't send a msg to the frontier node. That way, I won't be included in future queries.
     * 
     * @param msg
     */
    private void sendScoresToFrontier(ArrayList<RayMessage> unsortedMsgs) {
        // sort the msgs by their index
        	ArrayList<RayMessage> msgs = new ArrayList<>(Arrays.asList(new RayMessage[unsortedMsgs.size()]));
        	for (RayMessage msg : unsortedMsgs) {
        		if (msgs.get(msg.getPathIndex()) != null) {
//        			throw new IllegalArgumentException("should only have one msg for each path!  Original list: " + Arrays.deepToString(unsortedMsgs.toArray()));
        			continue;
        		}
        		msgs.set(msg.getPathIndex(), msg);
        	}
        	// remove any leftover nulls to hide our shame :(
        	for (int i=0; i < msgs.size(); i++) {
        		if (msgs.get(i) == null) {
        			msgs.remove(i--);
        		}
        	}
        	
        	VKmer id = getVertexId();
        	RayValue vertex = getVertexValue();
        	// all msgs should have the same total length and describe me as being at the same offset
        	int myOffset = msgs.get(0).getWalkOffsets().get(0);
        	int walkLength = msgs.get(0).getWalkLength();
        	VKmer frontierNode = new VKmer(msgs.get(0).getSourceVertexId());
        	VKmer accumulatedWalk = new VKmer(msgs.get(0).getAccumulatedWalkKmer());
        	HashMap<VKmer, Integer> visitCounter = msgs.get(0).getVisitCounter();
        	VKmer seed = new VKmer(msgs.get(0).getSeed());
        	// if the search has progressed beyond the reads I contain, don't do any scoring and don't report back
        	// to the frontier node. This effectively prunes me from the walk (I won't be queried anymore)
        	if (vertex.isOutOfRange(myOffset, walkLength, MAX_DISTANCE, seed)) {
        		if (id.equals(frontierNode)) {
        			// special case: I am the frontier node. Send an empty note just in case no 
        			// other nodes in the walk report back
        			outgoingMsg.reset();
        			outgoingMsg.setSeed(new VKmer(seed));
        			outgoingMsg.setMessageType(RayMessageType.AGGREGATE_SCORE);
        			outgoingMsg.setWalkLength(walkLength);
        			outgoingMsg.getWalkOffsets().add(myOffset);
        			outgoingMsg.getWalkIds().append(new VKmer(id));
        			outgoingMsg.setAccumulatedWalkKmer(new VKmer(accumulatedWalk));
        			outgoingMsg.setVisitCounter(new HashMap<>(visitCounter));
        			// include scores from each candidate path
        			for (RayMessage msg : msgs) {
        				if (msg.getSingleEndScores().size() > 0) {
        					outgoingMsg.getSingleEndScores().addAll(new ArrayList<>(msg.getSingleEndScores()));
        				}
        				if (msg.getPairedEndScores().size() > 0) {
        					outgoingMsg.getPairedEndScores().addAll(new ArrayList<>(msg.getPairedEndScores()));
        				}
        			} // TODO possible that NO scores come back from the candidates?
        			sendMsg(frontierNode, outgoingMsg);
        		}
        		return;
        	}

        	// get the smallest kmer in all the messages I've received
        	// since the candidates may be of different lengths, we have to use the shortest candidate
        	// that way, long candidates don't receive higher scores simply for being long
        	
        	int maxMsgLength = msgs.get(0).getToScoreKmer().getKmerLetterLength();
        	for (int i = 1; i < msgs.size(); i++) {
        		maxMsgLength = Math.min(maxMsgLength, msgs.get(i).getToScoreKmer().getKmerLetterLength());
        		//maxMsgLength = Math.max(maxMsgLength, msgs.get(i).getToScoreKmer().getKmerLetterLength());
        	}
        	int minLength = Math.min(MAX_DISTANCE, maxMsgLength);
        	minLength = minLength - Kmer.getKmerLength() + 1;
        	// I'm now allowed to score the first minLength kmers according to my readids
        
        	ArrayList<RayScores> singleEndScores = voteFromReads(true, vertex, vertex.getFlippedFromInitDir().get(seed), msgs,
        			myOffset, walkLength, minLength);
        	ArrayList<RayScores> pairedEndScores = HAS_PAIRED_END_READS ? voteFromReads(false, vertex,
        			vertex.getFlippedFromInitDir().get(seed), msgs, myOffset, walkLength, minLength) : null;
        	outgoingMsg.reset();
        	outgoingMsg.setSeed(new VKmer(seed));
        	outgoingMsg.setMessageType(RayMessageType.AGGREGATE_SCORE);
        	outgoingMsg.setWalkLength(walkLength);
        	outgoingMsg.getWalkOffsets().add(myOffset);
        	outgoingMsg.getWalkIds().append(new VKmer(id));
        	if (singleEndScores != null) {
        		outgoingMsg.getSingleEndScores().addAll(new ArrayList(singleEndScores));
        	}
        	//each message has a single-element list containing the candidates' total score of the accumulatedKmer
        	// we need to add that single element to the path score it corresponds to

        	if (id.equals(frontierNode)) { // only include the candidate's mutual scores once (here, in the frontier's scores)
        		for (int i = 0; i < msgs.size(); i++) {
        			if (msgs.get(i).getSingleEndScores().size() > 0) {
        				outgoingMsg.getSingleEndScores().get(i).addAll(new RayScores(msgs.get(i).getSingleEndScores().get(0)));
        			}
        		}
        	}
        	
        	if (pairedEndScores != null) {
        		outgoingMsg.getPairedEndScores().addAll(new ArrayList(pairedEndScores));
        	}
        	if (id.equals(frontierNode)) { // only include the candidate's mutual scores once (here, in the frontier's scores)
        		for (int i = 0; i < msgs.size(); i++) {
        			if (msgs.get(i).getPairedEndScores().size() > 0) {
        				outgoingMsg.getPairedEndScores().get(i).addAll(new RayScores(msgs.get(i).getPairedEndScores().get(0)));
        			}
        		}
        	}
        	outgoingMsg.setAccumulatedWalkKmer(new VKmer(accumulatedWalk));
        	outgoingMsg.setVisitCounter(new HashMap(visitCounter));
        	sendMsg(frontierNode, outgoingMsg);
//        	LOG.info("sending to frontier node: minLength: " + minLength + ", s-e: " + singleEndScores + ", p-e: "
//        			+ pairedEndScores);
        	//msgs.clear();
    
    }

    /**
     * use the reads present in this vertex to score the kmers in this message list.
     * For example, we are a walk node with readSeqs:
     * readHeadSet:
     * - (r1, offset 0, AAATTTGGGCCC)
     * - (r2, offset 2, ATTTGGTCCCCC)
     * and are asked to score two msgs with kmers and offsets:
     * candidateMsgs:
     * - (c1, 4, TTGGGCCC)
     * - (c2, 4, TTGGTCCC)
     * - (c3, 4, TTGGTCCCC)
     * with an (original) Kmer.length of K=4,
     * .
     * r1 appears earlier in the overall walk and so has more weight for ruleA
     * (specifically, ruleA_r1_factor = walkLength - offset 0 = 4)
     * whereas r2 is later in the walk (ruleA_r2_factor = walkLength - offset 2 = 2)
     * .
     * The msgKmerLength will be the shortest of the given kmers (in this case, |c1| = |c2| = 8),
     * meaning only the first 8 letters of the candidates will be compared.
     * .
     * candidate c1 matches r1 at TTGG, TGGG, GGGC, GGCC, and GCCC but r2 only at TTGG
     * so c1 has an overall score of:
     * - ruleA = 4 (matches) * 4 (r1_factor) + 1 (match) * 2 (r2_factor) = 18
     * - ruleB = 4 (matches) + 1 (match) = 5
     * .
     * candidate c2 matches r1 only at TTGG but matches r2 at TTGG, TGGT, GGTC, GTCC, and TCCC
     * so c2 has an overall score of:
     * - ruleA = 1 (match) * 4 (r1_factor) + 4 (matches) * 2 (r2_factor) = 10
     * - ruleB = 1 (match) + 4 (match) = 5
     * .
     * candidate c3 would have the same score as c2 since its last letter is skipped (msgKmerLength=8)
     * .
     * ruleC is the minimum non-zero ruleB contribution from individual nodes (of which, we are but one).
     * If 1 other node scored these same candidates but its 1 read (r3) only contained the end TCCC..., then
     * that read's ruleA_r3_factor = 1, making:
     * c1 have:
     * - ruleA = 18 + 0
     * - ruleB = 5 + 0
     * and c2 would have:
     * - ruleA = 10 + 1
     * - ruleB = 5 + 1
     * and finally,
     * - c1.ruleC = 5
     * - c2.ruleC = 1
     * As you can see, ruleC is actually penalizing the node that has more support here!
     * ruleC doesn't make sense when you're comparing nodes containing multiple kmers.
     * .
     * In this case, no candidate dominates the others (see {@link RayScores.dominates}).
     * .
     * .
     * The overall algorithm look like this:
     * For each message,
     * | for each read in the reads oriented with the search
     * | | run a sliding window of length (original) Kmer.length
     * | | | see if all the letters in the sliding window match the read
     * So somehow, we've turned this into a n**4 operation :(
     * // TODO for single-end reads, we could alternatively count how many letters in the VKmers match
     * // or we could base the score on something like edit distance
     */
    private static ArrayList<RayScores> voteFromReads(boolean singleEnd, RayValue vertex, boolean vertexFlipped,
            List<RayMessage> candidateMsgs, int nodeOffset, int walkLength, int msgKmerLength) {
        SortedSet<ReadHeadInfo> readSubsetOrientedWithSearch = getReadSubsetOrientedWithSearch(singleEnd, vertex,
                vertexFlipped, nodeOffset, walkLength);

        if (GenomixJobConf.debug && singleEnd) {
            LOG.info("candidates:");
            for (RayMessage msg : candidateMsgs) {
                LOG.info(msg.getEdgeTypeBackToFrontier() + ":" + msg.getToScoreId() + " = " + msg.getToScoreKmer()
                        + " passing through " + msg.candidatePathIds);
            }
            LOG.info("\noriented reads:\n" + readSubsetOrientedWithSearch);
            LOG.info("\nvertexFlipped: " + vertexFlipped + "\nunflipped: " + vertex.getUnflippedReadIds()
                    + "\nflipped: " + vertex.getFlippedReadIds());
        }

        // my contribution to each path's final score
        ArrayList<RayScores> pathScores = new ArrayList<>();
//        printReads(readSubsetOrientedWithSearch);
        for (RayMessage msg : candidateMsgs) {
            RayScores scores = new RayScores();
            // nothing like nested for loops 4 levels deep (!)
            // for single-end reads, we need the candidate in the same orientation as the search
            // for paired-end reads, the mate sequence is revcomp'ed; need the candidate the opposite orientation as the search
            VKmer candidateKmer = singleEnd ? msg.getToScoreKmer() : msg.getToScoreKmer().reverse();
            int ruleATotal = 0, ruleBTotal = 0, ruleCTotal = 0;
            for (ReadHeadInfo read : readSubsetOrientedWithSearch) {
            	boolean readMatch = false;
                for (int kmerIndex = 0; kmerIndex < msgKmerLength; kmerIndex++) {
                    boolean match = false;
                    // TODO we currently keep the score separately for each kmer we're considering
                    // ruleC is about the minimum value in the comparison of the single kmers adjacent to the frontier
                    // but we're currently using it as the minimum across many kmers.  We'll have to think about this 
                    // rule some more and what it means in a merged graph
                    int tmp = 0;
                    if (singleEnd) {
                        int localOffset = walkLength - nodeOffset + kmerIndex;
                        if (!vertexFlipped) {
                            localOffset -= read.getOffset();
                            tmp = read.getOffset();
                        } else {  
                            // need to flip the read so it points with the search
                        	//FIXME
                            localOffset -= (vertex.getKmerLength() - 1 - read.getOffset());
                            tmp = (vertex.getKmerLength() - 1 - read.getOffset());
                            //localOffset -= vertex.getKmerLength() - (read.getOffset() + readLength) - 1 ;
                            //int tmp = read.getOffset() - 
                            //if (!msg.getCandidateFlipped()) {
                            //read = new ReadHeadInfo(read);
                            //read.set(read.getMateId(), read.getLibraryId(), read.getReadId(), read.getOffset(), read
                                  //  .getThisReadSequence().reverse(), null);
                            //}
                        }
                        
                        if (read.getThisReadSequence().matchesExactly(localOffset, candidateKmer, kmerIndex,
                                Kmer.getKmerLength())) {
                            match = true;
                        }
                    } else {
                        int readLength = 10;
                        int outerDistanceMean = 21;
                        int outerDistanceStd = 0;
                        int mateStart = nodeOffset + read.getOffset() + outerDistanceMean - readLength;
                        int candidateInMate = walkLength - mateStart + kmerIndex;
                        tmp = read.getOffset();
                        // since read.thisSeq is in the same orientation as the search direction, 
                        // the mate sequence is flipped wrt search direction. we reverse it to be in 
                        // the same direction as the search direction.
                        if (read.getMateReadSequence().matchesInRange(candidateInMate - outerDistanceStd,
                                candidateInMate + outerDistanceStd + Kmer.getKmerLength(), candidateKmer, kmerIndex,
                                Kmer.getKmerLength())) {
                            match = true;
                        }
                    }
                    if (match) {
                        //ruleATotal += walkLength - nodeOffset - read.getOffset();
                        ruleATotal += walkLength - nodeOffset - tmp;
                        ruleBTotal++;
                        readMatch = true;
                    }
                }
                if (readMatch) {
                	ruleCTotal++;
                }
            }
            // TODO use the max over messages for each item
            scores.addRuleCounts(msg.getEdgeTypeBackToFrontier().mirror(), msg.getToScoreId(), ruleATotal, ruleBTotal,
                    ruleCTotal);
            if (scores.size() > 0) {
                pathScores.add(new RayScores(scores));
            } else {
                pathScores.add(null);
            }
        }
        if (pathScores.size() > 0) {
//        	printReads(readSubsetOrientedWithSearch);
        	StringBuilder b = new StringBuilder();
        	for (RayMessage msg : candidateMsgs) {
        		b.append(msg.getToScoreKmer()).append('\n');
        	}
//        	LOG.info("branches seen were:\n" + b.toString());
        }
        return pathScores;
    }

    private static void printReads(SortedSet<ReadHeadInfo> readSubsetOrientedWithSearch) {
    	StringBuilder b = new StringBuilder();
    	for (ReadHeadInfo read : readSubsetOrientedWithSearch) {
    		for (int i=0; i < read.getOffset(); i++) {
    			b.append(" ");
    		}
    		b.append(read.getThisReadSequence()).append('\n');
    	}
    	LOG.info("All reads oriented with search:\n" + b.toString());
	}

	// local variables for getReadSubsetOrientedWithSearch
    @SuppressWarnings("unchecked")
    private static final SortedSet<ReadHeadInfo> EMPTY_SORTED_SET = SetUtils.EMPTY_SORTED_SET;
	

    private static SortedSet<ReadHeadInfo> getReadSubsetOrientedWithSearch(boolean singleEnd, RayValue vertex,
            boolean vertexFlipped, int nodeOffset, int walkLength) {
        // select out the readheads that might apply to this query
        // here's some ascii art trying to explain what the heck is going on
        //
        // the * indicates the walk length and is the start offset of the candidates c1 and c2
        // 
        //  |0    |10    |20    |30    |40   
        //                              /--c1
        //  ------->  ------->  ------>*
        //                              \--c2
        //              A1-------------------->
        //  B1------------------->
        //  C1--->                       <----C2
        //    D1--->       <---D2
        //                E1-->                      <---E2
        //
        // if our read length is 25, only A1 and C1 will apply to our c1/c2 decision.
        // the point here is to skip scoring of any single-end reads too far to the left 
        // and any paired-end reads that aren't in the right range.
        // TODO do we want to separate different libraries into different lists?  
        // That way, we would be able to skip checking D1 in the query
        int myLength = vertex.getKmerLength() - Kmer.getKmerLength() + 1;
        int startOffset;
        int endOffset;
        if (singleEnd) {
            startOffset = walkLength - MAX_READ_LENGTH - nodeOffset;
            endOffset = walkLength - nodeOffset;
        } else {
            startOffset = walkLength - MAX_OUTER_DISTANCE - nodeOffset;
            endOffset = walkLength - MIN_OUTER_DISTANCE + MAX_READ_LENGTH - nodeOffset;
        }
        ReadHeadSet orientedReads = vertex.getUnflippedReadIds();
        if (vertexFlipped) {
            orientedReads = vertex.getFlippedReadIds();

            startOffset = myLength - startOffset;
            endOffset = myLength - endOffset;
            // swap start and end
            int tmpOffset = startOffset;
            startOffset = endOffset;
            endOffset = tmpOffset;
        }

        if (startOffset >= myLength || endOffset < 0) {
            return EMPTY_SORTED_SET;
        }
        //FIXME
       // return orientedReads.getOffSetRange(Math.max(0, startOffset), Math.min(myLength, endOffset));
        return orientedReads.getOffSetRange(0, vertex.getKmerLength());

    }

    // local variables for compareScoresAndPrune
    private ArrayList<RayScores> singleEndScores = new ArrayList<>();
    private ArrayList<RayScores> pairedEndScores = new ArrayList<>();
    private VKmerList walkIds = new VKmerList();
    private ArrayList<Integer> walkOffsets = new ArrayList<>();
    private VKmer accumulatedWalk = new VKmer();

    /**
     * I'm the frontier node and am now receiving the total scores from each node in the walk.
     * Aggregate all the scores from all the nodes by id, then determine if one of the candidates
     * dominates all the others. If so, send a prune msg to all the others and a continue msg to the dominator.
     * the walk stops under two conditions: 1) no node dominates all others or 2) this node is marked
     * STOP (my neighbor intersected another walk)
     * in the special case where the selected edge is myself (I am one of my neighbors-- a tandem repeat),
     * no edges will be pruned since if the repeat is favored, I would end up going the opposite direction and
     * possibly pruning the walk I just came from!
     * // TODO it seems that tandem repeats are prunable but if they dominate, the walk should stop here completely.
     * // Need to think about this a bit more.
     * @throws IOException 
     */
    private void compareScoresAndPrune(ArrayList<RayMessage> msgs) throws IOException {
        VKmer id = getVertexId();
        RayValue vertex = getVertexValue();
        int walkLength = 0;
        	// aggregate scores and walk info from all msgs
        	// the msgs incoming are one for each walk node and contain a list of scores, one for each path
        	singleEndScores.clear();
        	pairedEndScores.clear();
        	walkIds.clear();
        	walkOffsets.clear();
        	walkLength = msgs.get(0).getWalkLength();
        	VKmer seed = new VKmer(msgs.get(0).getSeed());
        	//int walkLength = msgs.get(0).getWalkLength();
        	for (RayMessage msg : msgs) {
        		if (walkLength != msg.getWalkLength()) {
        			throw new IllegalStateException("One of the messages didn't agree about the walk length! Expected "
        					+ walkLength + " but saw " + msg.getWalkLength());
        		}
        			// add each walk node's contribution to the single path represented by each list element
        			if (msg.getSingleEndScores().size() > 0) {
        				if (singleEndScores.size() == 0) {
        					//                    singleEndScores.addAll(Arrays.asList(new RayScores[msgs.size()])); // allocate placeholder null array
        					//                    for (int i = 0; i < singleEndScores.size(); i++) {
        					//                        singleEndScores.set(i, msg.getSingleEndScores().get(msg.getPathIndex()));
        					//                    }
                	
        					singleEndScores.addAll(new ArrayList(msg.getSingleEndScores()));
        				} else {
        					for (int i = 0; i < singleEndScores.size(); i++) {
                    	
        						//FIXME
        						//ELMIRA
        						/**
        						for (int j = 0; j < msg.getSingleEndScores().size() ; j++){
        							if (singleEndScores.get(i).getVkmer().equals(msg.getSingleEndScores().get(j).getVkmer()) &&
        									singleEndScores.get(i).getEdge() == msg.getSingleEndScores().get(j).getEdge()){
        								singleEndScores.get(i).addAll(new RayScores(msg.getSingleEndScores().get(j)));
        							}
        						}
        						**/
        					
                    		if ((i < msg.getSingleEndScores().size())){
                    			singleEndScores.get(i).addAll(msg.getSingleEndScores().get(i));
                    		}
        						 
        					}
        				}
        			}
            
        			if (msg.getPairedEndScores().size() > 0) {
        				if (pairedEndScores.size() == 0) {
        					//                    pairedEndScores.addAll(Arrays.asList(new RayScores[msgs.size()])); // allocate placeholder null array
        					//                    for (int i = 0; i < pairedEndScores.size(); i++) {
        					//                        pairedEndScores.set(i, msg.getSingleEndScores().get(msg.getPathIndex()));
        					//                    }
        					pairedEndScores.addAll(new ArrayList(msg.getPairedEndScores()));
        				} else {
        					for (int i = 0; i < pairedEndScores.size(); i++) {
        						//                        pairedEndScores.get(i).addAll(msg.getPairedEndScores().get(msg.getPathIndex()));
        						pairedEndScores.get(i).addAll(new RayScores(msg.getPairedEndScores().get(i)));
        					}
        				}
        			}
        			walkIds.append(new VKmer(msg.getWalkIds().getPosition(0)));
        			walkOffsets.add(msg.getWalkOffsets().get(0));
        			accumulatedWalk = msg.getAccumulatedWalkKmer();
        	}
        	HashMap <VKmer, Integer> visitCounter = msgs.get(0).getVisitCounter();
        	LOG.info("in prune for " + id + " scores are singleend: " + singleEndScores + " pairedend: " + pairedEndScores);

        	// we need to agree about the total number of paths we're considering...
        	int numSingleEndPaths = singleEndScores.size() > 0 ? singleEndScores.size() : -1;
        	int numPairedEndPaths = pairedEndScores.size() > 0 ? pairedEndScores.get(0).size() : -1;
        	if (numSingleEndPaths == -1 && numPairedEndPaths == -1) {
        		if (STORE_WALK){
        			storeWalk(walkIds, accumulatedWalk, seed);
        		}
        		LOG.info("failed to find a dominant edge (no scores available!). Started at "
        				+ msgs.get(0).getSourceVertexId() + " and will stop at " + id + " with total length: "
        				+ msgs.get(0).getWalkLength() + "\n>id " + id + "\n" + msgs.get(0).getAccumulatedWalkKmer());
        		//vertex.stopSearch = true;
        		return;
        	} else if (numSingleEndPaths != -1 && numPairedEndPaths != -1 && numSingleEndPaths != numPairedEndPaths) {
        		throw new IllegalStateException(
        				"single and paired end scores disagree about the total number of paths! (single: "
        						+ numSingleEndPaths + ", " + singleEndScores + "; paired: " + numPairedEndPaths + ", "
        						+ pairedEndScores);
        	}
        	int numPaths = numSingleEndPaths != -1 ? numSingleEndPaths : numPairedEndPaths;

        	VKmer dominantKmer = null;
        	EDGETYPE dominantEdgeType = null;
        	// need to compare each edge in this dir with every other one.  Unfortunately, this is ugly to do, requiring 
        	// us to iterate over edge types, then edges in those types, and keeping track of the indexes ourselves, etc :(
        	// this 4 loops are really just two for loops that are tracking 1) the index, 2) the edge type, and 3) the kmer
        	//
        	// the fact we need to compare all candidates vs all others can be captured by this statement:
        	//  (! c1.dominates(c2)) =!=> (c2.dominates(c1))
        	//
        	// that is, just because c1 doesn't dominate c2, doesn't mean that c2 dominates c1.
        	// the extra m factor makes it so we must compare all vs all.
        	//
        	// fortunately, we can quit comparing as soon as one edge doesn't dominate another edge. 
        	//
        	boolean equalPairedEdgeFound = false;
        	boolean equalSingleEdgeFound = false;
        	boolean dominantEdgeFound = false;
        	float coverage = vertex.getAverageCoverage();
        	RayScores tmpScores = new RayScores();

        	// look for a path that dominates all others.
        	// actually, some paths may share the same starting nodes; we don't have to dominate those ones

        	Rules dominantRules = null;
			// look for a paired-end dominator
        	if (pairedEndScores.size() > 0) {
        		for (int queryI = 0; queryI < numPaths; queryI++) {
        			equalPairedEdgeFound = false;
        			for (int targetJ = 0; targetJ < numPaths; targetJ++) {
        				if (queryI == targetJ) {
        					continue;
        				}
        				SimpleEntry<EDGETYPE, VKmer> queryBranch = pairedEndScores.get(queryI).getSingleKey();
        				SimpleEntry<EDGETYPE, VKmer> targetBranch = pairedEndScores.get(targetJ).getSingleKey();
        				if (!queryBranch.equals(targetBranch)) {
        					// not same initial candidate node... need to check these paths
        					tmpScores.clear();
        					tmpScores.addAll(pairedEndScores.get(queryI));
        					tmpScores.addAll(pairedEndScores.get(targetJ));
        					if (!tmpScores.dominates(queryBranch.getKey(), queryBranch.getValue(), targetBranch.getKey(),
        							targetBranch.getValue(), coverage)) {
        						equalPairedEdgeFound = true;
        						break;
        					}
        				}
        			}
        			if (!equalPairedEdgeFound) {
        				// this edge dominated all other edges.  Keep it as the winner
        				SimpleEntry<EDGETYPE, VKmer> queryBranch = pairedEndScores.get(queryI).getSingleKey();
        				dominantKmer = queryBranch.getValue();
        				dominantEdgeType = queryBranch.getKey();
        				dominantRules = pairedEndScores.get(queryI).getRules(queryBranch);
        				dominantEdgeFound = true;
        				break;
        			}
        			if (dominantEdgeFound) {
        				break;
        			}
        		}
        	}
        
        	// look for a single-end dominator if we didn't find a paired-end one
        	if (!dominantEdgeFound && singleEndScores.size() > 0) {;
        		for (int queryI = 0; queryI < numPaths; queryI++) {
        			equalSingleEdgeFound = false;
        			for (int targetJ = 0; targetJ < numPaths; targetJ++) {
        				if (queryI == targetJ) {
        					continue;
        				}
        				SimpleEntry<EDGETYPE, VKmer> queryBranch = singleEndScores.get(queryI).getSingleKey();
        				SimpleEntry<EDGETYPE, VKmer> targetBranch = singleEndScores.get(targetJ).getSingleKey();
        				if ((queryBranch != null) && (targetBranch != null)){
        					if (!queryBranch.equals(targetBranch)) {
        						// not same initial candidate node... need to check these paths
        						tmpScores.clear();
        						tmpScores.addAll(singleEndScores.get(queryI));
        						tmpScores.addAll(singleEndScores.get(targetJ));
        						if (!tmpScores.dominates(queryBranch.getKey(), queryBranch.getValue(), targetBranch.getKey(),
        								targetBranch.getValue(), coverage)) {
        							equalSingleEdgeFound = true;
        							break;
        						}
        					}
        				}
        			}
        			if (!equalSingleEdgeFound) {
        				// this edge dominated all other edges.  Keep it as the winner
        				SimpleEntry<EDGETYPE, VKmer> queryBranch = singleEndScores.get(queryI).getSingleKey();
        				if (queryBranch !=  null){
        					dominantKmer = queryBranch.getValue();
        					dominantEdgeType = queryBranch.getKey();
        					dominantRules = singleEndScores.get(queryI).getRules(queryBranch);
        					dominantEdgeFound = true;
        					break;
        				}
        			}
        		}
        	}
        	if (dominantEdgeFound) {
        		// if a dominant edge is found, all the others must be removed.
        		if (DELAY_PRUNE || SAVE_BEST_PATH) {
                	Entry<EDGETYPE, VKmer> edges = new SimpleEntry<EDGETYPE, VKmer>(dominantEdgeType, dominantKmer);
                	if (vertex.getOutgoingEdgesToKeep().containsKey(edges)) {
                		if (vertex.getOutgoingEdgesToKeep().get(edges) == null || vertex.getOutgoingEdgesToKeep().get(edges).ruleC < dominantRules.ruleC) {
                			vertex.getOutgoingEdgesToKeep().put(edges, dominantRules);
                		}
                	} else {
                		vertex.getOutgoingEdgesToKeep().put(edges, dominantRules);
                	}
        		} else if (REMOVE_OTHER_OUTGOING) {
        			for (EDGETYPE et : dominantEdgeType.dir().edgeTypes()) {
        				for (VKmer kmer : vertex.getEdges(et)) {
        					if (et != dominantEdgeType || !kmer.equals(dominantKmer)) {
        						outgoingMsg.reset();
        						outgoingMsg.setSeed(new VKmer(seed));
        						outgoingMsg.setMessageType(RayMessageType.PRUNE_EDGE);
        						outgoingMsg.setWalkIds(new VKmerList(walkIds));
        						outgoingMsg.setWalkLength(walkLength);
        						outgoingMsg.setEdgeTypeBackToFrontier(et.mirror());
        						outgoingMsg.setSourceVertexId(new VKmer(id));
        						sendMsg(kmer, outgoingMsg);
        						vertex.getEdges(et).remove(kmer);
        					}
        				}
        			}
        		}
        		// the walk is then passed on to the single remaining node
        		outgoingMsg.reset();
        		outgoingMsg.setSeed(new VKmer(seed));
        		outgoingMsg.setMessageType(RayMessageType.CONTINUE_WALK);
        		outgoingMsg.previousRules = dominantRules;
        		outgoingMsg.setEdgeTypeBackToFrontier(dominantEdgeType.mirror());
        		outgoingMsg.setWalkIds(new VKmerList(walkIds));
        		outgoingMsg.setWalkOffsets(walkOffsets);
        		outgoingMsg.setWalkLength(walkLength);
        		outgoingMsg.setAccumulatedWalkKmer(new VKmer(msgs.get(0).getAccumulatedWalkKmer()));
        		outgoingMsg.setFrontierFlipped(vertex.getFlippedFromInitDir().get(seed)); // TODO make sure this is correct
        		outgoingMsg.setCandidateFlipped(vertex.getFlippedFromInitDir().get(seed) ^ dominantEdgeType.causesFlip());
        		outgoingMsg.setVisitCounter(new HashMap<>(visitCounter));
        		sendMsg(dominantKmer, outgoingMsg);
        		// WriteToLog("dominantedgefound" ,msgs.get(0).getAccumulatedWalkKmer().toString() , getVertexId().toString());
        		LOG.info("dominant edge found: " + dominantEdgeType + ":" + dominantKmer);
        	} else {
        		HashSet<Entry<EDGETYPE, VKmer>> highNeighbors = getHighNeighbors(vertex, pairedEndScores, singleEndScores);
        		if (highNeighbors.size() >= 2) {
        			LOG.info("Going to do a split!");
        			List<Entry<Entry<EDGETYPE, VKmer>, VKmer>> newNodes = splitFrontier(getVertexId(), vertex, highNeighbors);
        			
        			// pass the walk along to all the high neighbors
        			for (Entry<Entry<EDGETYPE, VKmer>, VKmer> entry : newNodes) {
        				Entry<EDGETYPE, VKmer> highNeighbor = entry.getKey();
        				VKmer newId = entry.getValue();
        				outgoingMsg.reset();
                		outgoingMsg.setMessageType(RayMessageType.CONTINUE_WALK);
                		outgoingMsg.setEdgeTypeBackToFrontier(highNeighbor.getKey().mirror());
                		outgoingMsg.setWalkOffsets(walkOffsets);
                		outgoingMsg.setWalkLength(walkLength);
                		outgoingMsg.setAccumulatedWalkKmer(new VKmer(msgs.get(0).getAccumulatedWalkKmer()));
                		outgoingMsg.setFrontierFlipped(vertex.getFlippedFromInitDir().get(seed)); // TODO make sure this is correct
                		outgoingMsg.setCandidateFlipped(vertex.getFlippedFromInitDir().get(seed) ^ highNeighbor.getKey().causesFlip());
                		outgoingMsg.setVisitCounter(new HashMap<>(visitCounter));
                		// use previous walk except for the last item, which is now the newly created node attached to this candidate
                		outgoingMsg.setWalkIds(new VKmerList(walkIds));
                		outgoingMsg.getWalkIds().remove(outgoingMsg.getWalkIds().size() - 1);
                		outgoingMsg.getWalkIds().append(newId);
                		// use new node id as seed since the original seed will conflict in these two different walks
                		outgoingMsg.setSeed(newId); // TODO
//                		sendMsg(highNeighbor.getValue(), outgoingMsg);
                		LOG.info("After splitting " + getVertexId() + " into " + newId + ", passing CONTINUE_WALK along to " + highNeighbor);
        			}
        		} else {
	        		if (STORE_WALK){
	        			storeWalk(walkIds, msgs.get(0).getAccumulatedWalkKmer(), seed);
	        		}
	        		//vertex.stopSearch = true;
	        		LOG.info("failed to find a dominant edge. Started at " + msgs.get(0).getSourceVertexId()
	        				+ " and will stop at " + id + " with total length: " + msgs.get(0).getWalkLength() + " and curNode length: " + vertex.getKmerLength() + "\n>id " + id
	        				+ "\n" + msgs.get(0).getAccumulatedWalkKmer());
        		}
        	}       
    }

	private static HashSet<Entry<EDGETYPE, VKmer>> getHighNeighbors(RayValue frontier, ArrayList<RayScores> pairedEndScores, ArrayList<RayScores> singleEndScores) {
		HashSet<Entry<EDGETYPE, VKmer>> countHighNeighbors = new HashSet<>();
		if (frontier.getKmerLength() > MAX_DISTANCE) {
			for (List<RayScores> list : Arrays.asList(pairedEndScores, singleEndScores)) {
				for (RayScores score: list) {
					Entry<EDGETYPE, VKmer> scoreKey = score.getSingleKey();
					Rules r = score.getRules(scoreKey);
					if (r.ruleA > 15 && r.ruleB > 10 && r.ruleC >= 5) {
						countHighNeighbors.add(scoreKey);
					}
				}
			}
		}
		return countHighNeighbors;
	}
	
	private List<Entry<Entry<EDGETYPE, VKmer>, VKmer>> splitFrontier(VKmer frontierKmer, RayValue frontier, HashSet<Entry<EDGETYPE, VKmer>> highNeighbors) throws IOException {
		// duplicate the frontier node into one version per "high neighbor", rewiring s.t. the neighbor is only connected to one of the duplicated frontiers
		ArrayList<Entry<Entry<EDGETYPE, VKmer>, VKmer>> newNodes = new ArrayList<>();
		
		// remove all connections from this node to its neighbors, then delete it
		for (EDGETYPE et : EDGETYPE.values) {
			for (VKmer neighbor : frontier.getEdges(et)) {
                outgoingMsg.reset();
                outgoingMsg.setMessageType(RayMessageType.PRUNE_EDGE);
                outgoingMsg.setEdgeTypeBackToFrontier(et.mirror());
                outgoingMsg.setSourceVertexId(new VKmer(frontierKmer));
                sendMsg(neighbor, outgoingMsg);
//                LOG.info("Splitting node... sending message to remove reciprocal edge back to " + frontierKmer + " from " + neighbor);
			}
		}
		deleteVertex(frontierKmer);
		LOG.info("Splitting node... deleting " + frontierKmer);
		
		Random rng = new SecureRandom();
		for (Entry<EDGETYPE, VKmer> highNeighbor : highNeighbors) {
			// create a copy of myself for each neighbor. Use an almost certainly unique random id
//			StringBuilder b = new StringBuilder();
//			for (int i=0; i < creationCount; i++) {
//				b.append("A");
//			}
//			VKmer newId = new VKmer("ATGCTGTGCTGCTGATCGATCGTAGCTAGCTAGTCGATCGTAG" + b.toString());
//			creationCount++;
			VKmer newId = new VKmer(kmerSize + 5);
			rng.nextBytes(newId.getLetterBytes());
			newId = new VKmer(newId); // this fixes some crazy bug in rng...
			
			// deep copy using the stream read/write methods.  Thanks, Java.  That's a lot of wrappers.
			RayValue newValue = new RayValue();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			frontier.write(new DataOutputStream(baos));
			newValue.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
			
			// remove all edges in the neighbor direction except the neighbor
			for (EDGETYPE et : highNeighbor.getKey().dir().edgeTypes()) {
				newValue.getEdges(et).clear();
			}
			newValue.getEdges(highNeighbor.getKey()).append(highNeighbor.getValue());
			newNodes.add(new ImmutablePair<>(highNeighbor, newId));
			
			// tell pregelix about the new vertex
			Vertex newVertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
			newVertex.setVertexId(newId);
			newVertex.setVertexValue(newValue);
            addVertex(newId, newVertex);
			
			// tell all neighbors to be connected to this node
			for (EDGETYPE et : EDGETYPE.values) {
				for (VKmer neighbor : newValue.getEdges(et)) {
					outgoingMsg.reset();
					outgoingMsg.setMessageType(RayMessageType.ADD_EDGE);
					outgoingMsg.setEdgeTypeBackToFrontier(et.mirror());
					outgoingMsg.setSourceVertexId(new VKmer(newId));
					sendMsg(neighbor, outgoingMsg);
					LOG.info("Splitting node... sending message to add reciprocal edge back to " + newId + ", which is a copy of " + frontierKmer + " from " + neighbor);
				}
			}
		}
		return newNodes;
	}


	public void storeWalk(VKmerList walk, VKmer accumulatedWalk, VKmer seed) throws FileNotFoundException,
			UnsupportedEncodingException {
		writeOnFile(seed.toString());
		//writer.print("\n");
		writer.print(">");
		writer.print(walk.toString());
		writer.print("\n");
		writer.print(accumulatedWalk);
		writer.print("\n");
		writer.close();
	}
	
	/**
	public VKmerList loadWalkMap(final File directory) throws IOException{
		VKmerList walk = new VKmerList();
		VKmer node = new VKmer();
		for (final File file : directory.listFiles()) {
			walk.clear();
	        if ((!file.isDirectory() && ((file.getName().equals(getVertexId().toString() + ".txt")) || (file.getName().equals(getVertexId().reverse().toString() + "txt"))))) {
	        	String content = FileUtils.readFileToString(file);
	        	String [] parts = content.split("\n");
	        	String [] words = parts[1].split("\\P{Alpha}+");
	        	for (String word : words){
	        		if (word.length() > 0){
	        			node.setAsCopy(word);
	        			walk.append(node);
	        		}
	        	}
	        	return walk;
	        }
	    }
		 return null;
		
	}
	
	public VKmer loadAccWalk(final File directory) throws IOException{
		VKmer accWalk = new VKmer();
		for (final File file : directory.listFiles()) {
	        if ((!file.isDirectory() && ((file.getName().equals(getVertexId().toString() + ".txt")) || (file.getName().equals(getVertexId().reverse().toString() + "txt"))))) {
	        	String content = FileUtils.readFileToString(file);
	        	String [] parts = content.split("\n");
	        	accWalk.setAsCopy(parts[2]);
	        	return accWalk;
	        }
	    }
		 return null;
		
	}
	**/
	
    public static PregelixJob getConfiguredJob(
            GenomixJobConf conf,
            Class<? extends DeBruijnGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass)
            throws IOException {
        PregelixJob job = DeBruijnGraphCleanVertex.getConfiguredJob(conf, vertexClass);
        job.setVertexInputFormatClass(NodeToRayVertexInputFormat.class);
        job.setVertexOutputFormatClass(RayVertexToNodeOutputFormat.class);
        return job;
    }

    public static Logger LOG = Logger.getLogger(RayVertex.class.getName());

    @SuppressWarnings("deprecation")
    public static int calculateScoreThreshold(Counters statsCounters, Float topFraction, Integer topNumber,
            String scoreKey) {
        if ((topFraction == null && topNumber == null) || (topFraction != null && topNumber != null)) {
            throw new IllegalArgumentException("Please specify either topFraction or topNumber, but not both!");
        }
        TreeMap<Integer, Long> scoreHistogram = new TreeMap<>();
        int total = 0;
        for (Counter c : statsCounters.getGroup(scoreKey + "-bins")) { // counter name is index; counter value is the count for this index
            Integer X = Integer.parseInt(c.getName());
            if (scoreHistogram.get(X) != null) {
                scoreHistogram.put(X, scoreHistogram.get(X) + c.getCounter());
            } else {
                scoreHistogram.put(X, c.getCounter());
            }
            total += c.getCounter();
        }

        if (topNumber == null) {
            topNumber = (int) (total * topFraction);
        }

        long numSeen = 0;
        int ignore = 0;
        Integer lastSeen = null;
        for (Entry<Integer, Long> e : scoreHistogram.descendingMap().entrySet()) {
        	//ignore++;
        	//if (ignore > (scoreHistogram.size()/ (1.5))){
        		numSeen += e.getValue();
        		lastSeen = e.getKey();
        	//}
            if (numSeen >= topNumber) {
                break;
            }
        }
        return lastSeen;
    }
    
    
    
}
