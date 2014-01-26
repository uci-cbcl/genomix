package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.apache.commons.collections.SetUtils;
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
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class RayVertex extends DeBruijnGraphCleanVertex<RayValue, RayMessage> {
    private static DIR INITIAL_DIRECTION;
    private static String SEED_ID;
    private Integer SEED_SCORE_THRESHOLD;
    private Integer SEED_LENGTH_THRESHOLD;
    private int COVERAGE_DIST_NORMAL_MEAN;
    private int COVERAGE_DIST_NORMAL_STD;
    private static boolean HAS_PAIRED_END_READS;
    private static int MAX_READ_LENGTH;
    private static int MAX_OUTER_DISTANCE;
    private static int MIN_OUTER_DISTANCE;
    private static int MAX_DISTANCE; // the max(readlengths, outerdistances)

    public static final boolean REMOVE_OTHER_OUTGOING = true; // whether to remove other outgoing branches when a dominant edge is chosen
    public static final boolean REMOVE_OTHER_INCOMING = true; // whether to remove other incoming branches when a dominant edge is chosen
    public static final boolean CANDIDATES_SCORE_WALK = false; // whether to have the candidates score the walk
    private static final boolean EXPAND_CANDIDATE_BRANCHES = true; // whether to get kmer from all possible candidate branches

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

        if (conf.get(GenomixJobConf.SCAFFOLD_SEED_ID) != null) {
            SEED_ID = conf.get(GenomixJobConf.SCAFFOLD_SEED_ID);
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

        if (getSuperstep() == 1) {
            // manually clear state
            getVertexValue().visited = false;
            getVertexValue().intersection = false;
            // FIXME
            if (INITIAL_DIRECTION == DIR.REVERSE){
            	getVertexValue().flippedFromInitialDirection = true;
            }else {
            	getVertexValue().flippedFromInitialDirection = false;
            }
            getVertexValue().stopSearch = false;
        }
    }

    @Override
    public void compute(Iterator<RayMessage> msgIterator) throws Exception {
        if (getSuperstep() == 1) {
            if (isStartSeed()) {
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
        if (SEED_ID != null) {
            return SEED_ID.equals(getVertexId().toString());
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
        if (INITIAL_DIRECTION == DIR.FORWARD) {
            initialMsg.setFrontierFlipped(false);
            initialMsg.setEdgeTypeBackToFrontier(EDGETYPE.RR);
        } else {
            initialMsg.setFrontierFlipped(true);
            initialMsg.setEdgeTypeBackToFrontier(EDGETYPE.FF);
            //FIXME
            initialMsg.setCandidateFlipped(true);
        }
        return new ArrayList<RayMessage>(Collections.singletonList(initialMsg)).iterator();
    }

    // local variables for scaffold
    private ArrayList<RayMessage> requestScoreMsgs = new ArrayList<>();
    private ArrayList<RayMessage> aggregateScoreMsgs = new ArrayList<>();

    private void scaffold(Iterator<RayMessage> msgIterator) {
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
                    startBranchComparison(msg);
                    break;
                case REQUEST_CANDIDATE_KMER:
                    sendCandidatesToFrontier(msg);
                    break;
                case ASSEMBLE_CANDIDATES:
                    vertex.getCandidateMsgs().add(new RayMessage(msg));
                    vertex.pendingCandidateBranches--;
                    LOG.info("recieved complete candidate. total pending searches:" + vertex.pendingCandidateBranches);
                    break;
                case REQUEST_SCORE:
                    // batch-process these (have to truncate to min length)
                    requestScoreMsgs.add(new RayMessage(msg));
                    break;
                case AGGREGATE_SCORE:
                    aggregateScoreMsgs.add(new RayMessage(msg));
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
                    vertex.visited = false;
                    break;
                case STOP:
                    // I am a frontier node but one of my neighbors was already included in a different walk.
                    // that neighbor is marked "intersection" and I need to remember the stopped state.  No path
                    // can continue out of me in the future, even when I receive aggregate scores
                    vertex.stopSearch = true;
                    break;
                case UPDATE_FORK_COUNT:
                    vertex.pendingCandidateBranches += msg.getNumberOfForks();
                    LOG.info("new candidate registered. total pending searches:" + vertex.pendingCandidateBranches);
                    break;
            }
        }
        if (requestScoreMsgs.size() > 0) {
            sendScoresToFrontier(requestScoreMsgs);
        }
        if (aggregateScoreMsgs.size() > 0) {
            compareScoresAndPrune(aggregateScoreMsgs);
        }
        if (vertex.pendingCandidateBranches != null && vertex.pendingCandidateBranches == 0
                && vertex.getCandidateMsgs().size() > 0) {
            sendCandidateKmersToWalkNodes(vertex.getCandidateMsgs());
            vertex.getCandidateMsgs().clear();
        }
    }

    private void sendCandidateKmersToWalkNodes(ArrayList<RayMessage> candidateMsgs) {
        LOG.info("sending " + candidateMsgs.size() + " candidates to " + candidateMsgs.get(0).getWalkIds().size()
                + " walk nodes");
        for (int candIndex = 0; candIndex < candidateMsgs.size(); candIndex++) {
            RayMessage msg = candidateMsgs.get(candIndex);
            for (int walkIndex = 0; walkIndex < msg.getWalkIds().size(); walkIndex++) {
                outgoingMsg.setAsCopy(msg);
                outgoingMsg.setMessageType(RayMessageType.REQUEST_SCORE);
                outgoingMsg.getWalkOffsets().clear();
                outgoingMsg.getWalkOffsets().add(msg.getWalkOffsets().get(walkIndex)); // the offset of the destination node
                outgoingMsg.getWalkIds().clear();
                outgoingMsg.getWalkIds().append(msg.getWalkIds().getPosition(walkIndex));
                outgoingMsg.setPathIndex(candIndex);
                sendMsg(msg.getWalkIds().getPosition(walkIndex), outgoingMsg);
            }
        }
    }

    private void startBranchComparison(RayMessage msg) {
        // I'm the new frontier node... time to do some pruning
        VKmer id = getVertexId();
        RayValue vertex = getVertexValue();

        // must explicitly check if this node was visited already (would have happened in 
        // this iteration as a previously processed msg!)
        if (vertex.visited) {
            vertex.intersection = true;
            vertex.stopSearch = true;
            LOG.info("start branch comparison had to stop at " + id + " with total length: " + msg.getWalkLength()
                    + "\n>id " + id + "\n" + msg.getAccumulatedWalkKmer());
            return;
        }
        vertex.visited = true;
        // I am the new frontier but this message is coming from the previous frontier; I was the "candidate"
        vertex.flippedFromInitialDirection = msg.getCandidateFlipped();
        DIR nextDir = msg.getEdgeTypeBackToFrontier().mirror().neighborDir();

        if (REMOVE_OTHER_INCOMING && getSuperstep() > 1) {
            removeOtherIncomingEdges(msg, id, vertex);
        }

        msg.visitNode(id, vertex, INITIAL_DIRECTION);

        if (vertex.degree(nextDir) == 0) {
            // this walk has reached a dead end!  nothing to do in this case.
            LOG.info("reached dead end at " + id + " with total length: " + msg.getWalkLength() + "\n>id " + id + "\n"
                    + msg.getAccumulatedWalkKmer());
            return;
        } else if (vertex.degree(nextDir) == 1) {
            // one neighbor -> just send him a continue msg w/ me added to the list
            NeighborInfo next = vertex.getSingleNeighbor(nextDir);
            msg.setEdgeTypeBackToFrontier(next.et.mirror());
            msg.setFrontierFlipped(vertex.flippedFromInitialDirection);
            msg.setCandidateFlipped(vertex.flippedFromInitialDirection ^ next.et.mirror().causesFlip());
            sendMsg(next.kmer, msg);
            LOG.info("bouncing over path node: " + id);
        } else {
            // 2+ neighbors -> start evaluating candidates via a REQUEST_KMER msg
            msg.setMessageType(RayMessageType.REQUEST_CANDIDATE_KMER);
            msg.setFrontierFlipped(vertex.flippedFromInitialDirection);
            msg.setSourceVertexId(id);
            for (EDGETYPE et : nextDir.edgeTypes()) {
                for (VKmer next : vertex.getEdges(et)) {
                    msg.setEdgeTypeBackToFrontier(et.mirror());
                    msg.setEdgeTypeBackToPrev(et.mirror());
                    msg.setCandidateFlipped(vertex.flippedFromInitialDirection ^ et.mirror().causesFlip());
                    sendMsg(next, msg);
                    LOG.info("evaluating branch: " + et + ":" + next);
                }
            }

            // remember how many total candidate branches to expect
            vertex.pendingCandidateBranches = vertex.degree(nextDir);
        }
    }

    private void removeOtherIncomingEdges(RayMessage msg, VKmer id, RayValue vertex) {
        // remove all other "incoming" nodes except the walk we just came from. on first iteration, we haven't really "hopped" from anywhere 
        // find the most recent node in the walk-- this was the frontier last iteration
        int lastOffset = Integer.MIN_VALUE;
        int lastIndex = -1;
        for (int i = 0; i < msg.getWalkIds().size(); i++) {
            if (msg.getWalkOffsets().get(i) > lastOffset) {
                lastOffset = msg.getWalkOffsets().get(i);
                lastIndex = i;
            }
        }
        VKmer lastId = msg.getWalkIds().getPosition(lastIndex);
        DIR prevDir = msg.getEdgeTypeBackToFrontier().dir();
        for (EDGETYPE et : prevDir.edgeTypes()) {
            Iterator<VKmer> it = vertex.getEdges(et).iterator();
            while (it.hasNext()) {
                VKmer other = it.next();
                if (et != msg.getEdgeTypeBackToFrontier() || !other.equals(lastId)) {
                    // only keep the dominant edge
                    outgoingMsg.reset();
                    outgoingMsg.setMessageType(RayMessageType.PRUNE_EDGE);
                    outgoingMsg.setEdgeTypeBackToFrontier(et.mirror());
                    outgoingMsg.setSourceVertexId(id);
                    sendMsg(other, outgoingMsg);
                    it.remove();
                    //                        vertex.getEdges(et).remove(other);
                }
            }
        }
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
     */
    private void sendCandidatesToFrontier(RayMessage msg) {
        VKmer id = getVertexId();
        RayValue vertex = getVertexValue();
        DIR nextDir = msg.getEdgeTypeBackToPrev().mirror().neighborDir();

        // already visited -> the frontier must stop!
        if (vertex.visited) {
            vertex.intersection = true;
            outgoingMsg.reset();
            outgoingMsg.setMessageType(RayMessageType.STOP);
            sendMsg(msg.getSourceVertexId(), outgoingMsg);
            return;
        }

        ArrayList<RayScores> singleEndScores = null;
        ArrayList<RayScores> pairedEndScores = null;
        if (CANDIDATES_SCORE_WALK) {
            // this candidate node needs to score the accumulated walk
            vertex.flippedFromInitialDirection = msg.getCandidateFlipped();

            tmpCandidate.reset();
            // pretend that the candidate is the frontier and the frontier is the candidate
            tmpCandidate.setFrontierFlipped(vertex.flippedFromInitialDirection);
            tmpCandidate.setEdgeTypeBackToFrontier(msg.getEdgeTypeBackToFrontier().mirror());
            tmpCandidate.setToScoreKmer(msg.getAccumulatedWalkKmer());
            tmpCandidate.setToScoreId(id);
            tmpCandidate.setCandidateFlipped(false); // TODO check this... the accumulated kmer is always in the same dir as the search, right?
            // TODO need to truncate to only a subset of readids if the MAX_DISTANCE isn't being used for max candidate search length
            singleEndScores = voteFromReads(true, vertex, !vertex.flippedFromInitialDirection,
                    Collections.singletonList(tmpCandidate), 0, vertex.getKmerLength(), msg.getAccumulatedWalkKmer()
                            .getKmerLetterLength());
            pairedEndScores = HAS_PAIRED_END_READS ? voteFromReads(false, vertex, !vertex.flippedFromInitialDirection,
                    Collections.singletonList(tmpCandidate), 0, vertex.getKmerLength(), msg.getAccumulatedWalkKmer()
                            .getKmerLetterLength()) : null;

            // TODO keep these scores?

            vertex.flippedFromInitialDirection = null;
        }

        outgoingMsg.reset();
        if (singleEndScores != null) {
            outgoingMsg.getSingleEndScores().addAll(singleEndScores);
        }
        if (pairedEndScores != null) {
            outgoingMsg.getPairedEndScores().addAll(pairedEndScores);
        }
        // keep previous msg details
        outgoingMsg.setFrontierFlipped(msg.getFrontierFlipped());
        outgoingMsg.setEdgeTypeBackToFrontier(msg.getEdgeTypeBackToFrontier());
        outgoingMsg.setSourceVertexId(msg.getSourceVertexId()); // frontier node
        outgoingMsg.setWalkLength(msg.getWalkLength());
        outgoingMsg.setAccumulatedWalkKmer(msg.getAccumulatedWalkKmer());
        outgoingMsg.setWalkIds(msg.getWalkIds());
        outgoingMsg.setWalkOffsets(msg.getWalkOffsets());

        // get kmer and id to score in walk nodes
        boolean readyToScore = false;
        if (EXPAND_CANDIDATE_BRANCHES) {
            // need to add this vertex to the candidate walk then check if we've looked far enough.
            // also notify the frontier of additional branches
            if (msg.getToScoreId().getKmerLetterLength() == 0) {
                outgoingMsg.setToScoreId(id);
                outgoingMsg.setToScoreKmer(!msg.getCandidateFlipped() ? vertex.getInternalKmer() : vertex
                        .getInternalKmer().reverse());
            } else {
                outgoingMsg.setToScoreId(msg.getToScoreId());
                outgoingMsg.setToScoreKmer(new VKmer(msg.getToScoreKmer()));
                EDGETYPE accumulatedCandidateToVertexET = !msg.getCandidateFlipped() ? EDGETYPE.FF : EDGETYPE.FR;
                outgoingMsg.getToScoreKmer().mergeWithKmerInDir(accumulatedCandidateToVertexET, Kmer.getKmerLength(),
                        vertex.getInternalKmer());
            }
            outgoingMsg.candidatePathIds.setAsCopy(msg.candidatePathIds);
            outgoingMsg.candidatePathIds.append(id);

            // pass this kmer along to my adjacent branches
            if (outgoingMsg.getToScoreKmer().getKmerLetterLength() >= MAX_DISTANCE || vertex.degree(nextDir) == 0) {
                // this branch doesn't need to search any longer-- pass back to frontier
                readyToScore = true;
            }
        } else {
            outgoingMsg.setToScoreId(id); // candidate node (me)
            outgoingMsg.setToScoreKmer(!msg.getCandidateFlipped() ? vertex.getInternalKmer() : vertex.getInternalKmer()
                    .reverse());
            readyToScore = true;
        }

        if (readyToScore) {
            // send this complete candidate to the frontier node
            outgoingMsg.setMessageType(RayMessageType.ASSEMBLE_CANDIDATES);
            sendMsg(msg.getSourceVertexId(), outgoingMsg);
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
                    outgoingMsg.setCandidateFlipped(msg.getCandidateFlipped() ^ et.causesFlip());
                    sendMsg(neighbor, outgoingMsg);
                }
            }

            // notify the frontier about the number of forks we generated (so they know when to stop waiting)
            if (vertex.degree(nextDir) > 1) {
                outgoingMsg.reset();
                outgoingMsg.setMessageType(RayMessageType.UPDATE_FORK_COUNT);
                outgoingMsg.setNumberOfForks(vertex.degree(nextDir) - 1);
                sendMsg(msg.getSourceVertexId(), outgoingMsg);
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
                throw new IllegalArgumentException("should only have one msg for each path!");
            }
            msgs.set(msg.getPathIndex(), msg);
        }

        VKmer id = getVertexId();
        RayValue vertex = getVertexValue();

        // all msgs should have the same total length and describe me as being at the same offset
        int myOffset = msgs.get(0).getWalkOffsets().get(0);
        int walkLength = msgs.get(0).getWalkLength();
        VKmer frontierNode = msgs.get(0).getSourceVertexId();
        VKmer accumulatedWalk = msgs.get(0).getAccumulatedWalkKmer();

        // if the search has progressed beyond the reads I contain, don't do any scoring and don't report back
        // to the frontier node. This effectively prunes me from the walk (I won't be queried anymore)
        if (vertex.isOutOfRange(myOffset, walkLength, MAX_DISTANCE)) {
            if (id.equals(frontierNode)) {
                // special case: I am the frontier node. Send an empty note just in case no 
                // other nodes in the walk report back
                outgoingMsg.reset();
                outgoingMsg.setMessageType(RayMessageType.AGGREGATE_SCORE);
                outgoingMsg.setWalkLength(walkLength);
                outgoingMsg.getWalkOffsets().add(myOffset);
                outgoingMsg.getWalkIds().append(id);
                outgoingMsg.setAccumulatedWalkKmer(accumulatedWalk);
                // include scores from each candidate path
                for (RayMessage msg : msgs) {
                    if (msg.getSingleEndScores().size() > 0) {
                        outgoingMsg.getSingleEndScores().addAll(msg.getSingleEndScores());
                    }
                    if (msg.getPairedEndScores().size() > 0) {
                        outgoingMsg.getPairedEndScores().addAll(msg.getPairedEndScores());
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
            maxMsgLength = Math.max(maxMsgLength, msgs.get(i).getToScoreKmer().getKmerLetterLength());
        }
        int minLength = Math.min(MAX_DISTANCE, maxMsgLength);
        //FIXME
        //minLength = vertex.getKmerLength() + minLength - Kmer.getKmerLength() + 1 - Kmer.getKmerLength() -1;
        minLength = minLength - Kmer.getKmerLength() + 1;
        // I'm now allowed to score the first minLength kmers according to my readids
        ArrayList<RayScores> singleEndScores = voteFromReads(true, vertex, vertex.flippedFromInitialDirection, msgs,
                myOffset, walkLength, minLength);
        ArrayList<RayScores> pairedEndScores = HAS_PAIRED_END_READS ? voteFromReads(false, vertex,
                vertex.flippedFromInitialDirection, msgs, myOffset, walkLength, minLength) : null;

        outgoingMsg.reset();
        outgoingMsg.setMessageType(RayMessageType.AGGREGATE_SCORE);
        outgoingMsg.setWalkLength(walkLength);
        outgoingMsg.getWalkOffsets().add(myOffset);
        outgoingMsg.getWalkIds().append(id);
        if (singleEndScores != null) {
            outgoingMsg.getSingleEndScores().addAll(singleEndScores);
        }
        // each message has a single-element list containing the candidates' total score of the accumulatedKmer
        // we need to add that single element to the path score it corresponds to
        if (id.equals(frontierNode)) { // only include the candidate's mutual scores once (here, in the frontier's scores)
            for (int i = 0; i < msgs.size(); i++) {
                if (msgs.get(i).getSingleEndScores().size() > 0) {
                    outgoingMsg.getSingleEndScores().get(i).addAll(msgs.get(i).getSingleEndScores().get(0));
                }
            }
        }
        if (pairedEndScores != null) {
            outgoingMsg.getPairedEndScores().addAll(pairedEndScores);
        }
        if (id.equals(frontierNode)) { // only include the candidate's mutual scores once (here, in the frontier's scores)
            for (int i = 0; i < msgs.size(); i++) {
                if (msgs.get(i).getPairedEndScores().size() > 0) {
                    outgoingMsg.getPairedEndScores().get(i).addAll(msgs.get(i).getPairedEndScores().get(0));
                }
            }
        }
        outgoingMsg.setAccumulatedWalkKmer(accumulatedWalk);
        sendMsg(frontierNode, outgoingMsg);
        LOG.info("sending to frontier node: minLength: " + minLength + ", s-e: " + singleEndScores + ", p-e: "
                + pairedEndScores);
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
    	//FIXME
    	//if (INITIAL_DIRECTION == DIR.REVERSE){
    	//	vertexFlipped = !vertexFlipped;
    	//}
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
        for (RayMessage msg : candidateMsgs) {
            RayScores scores = new RayScores();
            // nothing like nested for loops 4 levels deep (!)
            // for single-end reads, we need the candidate in the same orientation as the search
            // for paired-end reads, the mate sequence is revcomp'ed; need the candidate the opposite orientation as the search
            VKmer candidateKmer = singleEnd ? msg.getToScoreKmer() : msg.getToScoreKmer().reverse();
            int ruleATotal = 0, ruleBTotal = 0, ruleCTotal = 0;
            for (ReadHeadInfo read : readSubsetOrientedWithSearch) {
                for (int kmerIndex = 0; kmerIndex < msgKmerLength; kmerIndex++) {
                    boolean match = false;
                    // TODO we currently keep the score separately for each kmer we're considering
                    // ruleC is about the minimum value in the comparison of the single kmers adjacent to the frontier
                    // but we're currently using it as the minimum across many kmers.  We'll have to think about this 
                    // rule some more and what it means in a merged graph
                    int tmp = 0;
                    if (singleEnd) {
                    	// FIXME
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
                        ruleCTotal++;
                    }
                }
            }
            // TODO use the max over messages for each item
            scores.addRuleCounts(msg.getEdgeTypeBackToFrontier().mirror(), msg.getToScoreId(), ruleATotal, ruleBTotal,
                    ruleCTotal);
            if (scores.size() > 0) {
                pathScores.add(scores);
            } else {
                pathScores.add(null);
            }
        }
        return pathScores;
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
        
       // return orientedReads.getOffSetRange(Math.max(0, startOffset), Math.min(myLength, endOffset));
        return orientedReads.getOffSetRange(0, vertex.getKmerLength());

    }

    // local variables for compareScoresAndPrune
    private ArrayList<RayScores> singleEndScores = new ArrayList<>();
    private ArrayList<RayScores> pairedEndScores = new ArrayList<>();
    private VKmerList walkIds = new VKmerList();
    private ArrayList<Integer> walkOffsets = new ArrayList<>();

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
     */
    private void compareScoresAndPrune(ArrayList<RayMessage> msgs) {
        VKmer id = getVertexId();
        RayValue vertex = getVertexValue();

        if (vertex.stopSearch) {
            // one of my candidate nodes was already visited by a different walk
            // I can't proceed with the prune and I have to stop the search entirely :(
            LOG.info("prune and search had to stop at " + id + " with total length " + msgs.get(0).getWalkLength()
                    + "\nkmer: " + msgs.get(0).getAccumulatedWalkKmer());
            return;
        }

        // aggregate scores and walk info from all msgs
        // the msgs incoming are one for each walk node and contain a list of scores, one for each path
        singleEndScores.clear();
        pairedEndScores.clear();
        walkIds.clear();
        walkOffsets.clear();
        int walkLength = msgs.get(0).getWalkLength();
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
                    singleEndScores.addAll(msg.getSingleEndScores());
                } else {
                    for (int i = 0; i < singleEndScores.size(); i++) {
                        //                        singleEndScores.get(i).addAll(msg.getSingleEndScores().get(msg.getPathIndex()));
                        singleEndScores.get(i).addAll(msg.getSingleEndScores().get(i));
                    }
                }
            }
            if (msg.getPairedEndScores().size() > 0) {
                if (pairedEndScores.size() == 0) {
                    //                    pairedEndScores.addAll(Arrays.asList(new RayScores[msgs.size()])); // allocate placeholder null array
                    //                    for (int i = 0; i < pairedEndScores.size(); i++) {
                    //                        pairedEndScores.set(i, msg.getSingleEndScores().get(msg.getPathIndex()));
                    //                    }
                    pairedEndScores.addAll(msg.getPairedEndScores());
                } else {
                    for (int i = 0; i < pairedEndScores.size(); i++) {
                        //                        pairedEndScores.get(i).addAll(msg.getPairedEndScores().get(msg.getPathIndex()));
                        pairedEndScores.get(i).addAll(msg.getPairedEndScores().get(i));
                    }
                }
            }
            walkIds.append(msg.getWalkIds().getPosition(0));
            walkOffsets.add(msg.getWalkOffsets().get(0));
        }
        LOG.info("in prune for " + id + " scores are singleend: " + singleEndScores + " pairedend: " + pairedEndScores);

        // we need to agree about the total number of paths we're considering...
        int numSingleEndPaths = singleEndScores.size() > 0 ? singleEndScores.size() : -1;
        int numPairedEndPaths = pairedEndScores.size() > 0 ? pairedEndScores.get(0).size() : -1;
        if (numSingleEndPaths == -1 && numPairedEndPaths == -1) {
            LOG.info("failed to find a dominant edge (no scores available!). Started at "
                    + msgs.get(0).getSourceVertexId() + " and will stop at " + id + " with total length: "
                    + msgs.get(0).getWalkLength() + "\n>id " + id + "\n" + msgs.get(0).getAccumulatedWalkKmer());
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
                    dominantEdgeFound = true;
                    break;
                }
                if (dominantEdgeFound) {
                    break;
                }
            }
        }

        // look for a single-end dominator if we didn't find a paired-end one
        if (!dominantEdgeFound && singleEndScores.size() > 0) {
            for (int queryI = 0; queryI < numPaths; queryI++) {
                equalSingleEdgeFound = false;
                for (int targetJ = 0; targetJ < numPaths; targetJ++) {
                    if (queryI == targetJ) {
                        continue;
                    }
                    SimpleEntry<EDGETYPE, VKmer> queryBranch = singleEndScores.get(queryI).getSingleKey();
                    SimpleEntry<EDGETYPE, VKmer> targetBranch = singleEndScores.get(targetJ).getSingleKey();
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
                if (!equalSingleEdgeFound) {
                    // this edge dominated all other edges.  Keep it as the winner
                    SimpleEntry<EDGETYPE, VKmer> queryBranch = singleEndScores.get(queryI).getSingleKey();
                    dominantKmer = queryBranch.getValue();
                    dominantEdgeType = queryBranch.getKey();
                    dominantEdgeFound = true;
                    break;
                }
            }
        }

        if (dominantEdgeFound) {
            // if a dominant edge is found, all the others must be removed.
            if (REMOVE_OTHER_OUTGOING) {
                for (EDGETYPE et : dominantEdgeType.dir().edgeTypes()) {
                    for (VKmer kmer : vertex.getEdges(et)) {
                        if (et != dominantEdgeType || !kmer.equals(dominantKmer)) {
                            outgoingMsg.reset();
                            outgoingMsg.setMessageType(RayMessageType.PRUNE_EDGE);
                            outgoingMsg.setEdgeTypeBackToFrontier(et.mirror());
                            outgoingMsg.setSourceVertexId(id);
                            sendMsg(kmer, outgoingMsg);
                            vertex.getEdges(et).remove(kmer);
                        }
                    }
                }
            }
            // the walk is then passed on to the single remaining node
            outgoingMsg.reset();
            outgoingMsg.setMessageType(RayMessageType.CONTINUE_WALK);
            outgoingMsg.setEdgeTypeBackToFrontier(dominantEdgeType.mirror());
            outgoingMsg.setWalkIds(walkIds);
            outgoingMsg.setWalkOffsets(walkOffsets);
            outgoingMsg.setWalkLength(walkLength);
            outgoingMsg.setAccumulatedWalkKmer(msgs.get(0).getAccumulatedWalkKmer());
            outgoingMsg.setFrontierFlipped(vertex.flippedFromInitialDirection); // TODO make sure this is correct
            outgoingMsg.setCandidateFlipped(vertex.flippedFromInitialDirection ^ dominantEdgeType.causesFlip());
            sendMsg(dominantKmer, outgoingMsg);
            LOG.info("dominant edge found: " + dominantEdgeType + ":" + dominantKmer);
        } else {
            LOG.info("failed to find a dominant edge. Started at " + msgs.get(0).getSourceVertexId()
                    + " and will stop at " + id + " with total length: " + msgs.get(0).getWalkLength() + "\n>id " + id
                    + "\n" + msgs.get(0).getAccumulatedWalkKmer());
        }
    }

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
        Integer lastSeen = null;
        for (Entry<Integer, Long> e : scoreHistogram.descendingMap().entrySet()) {
            numSeen += e.getValue();
            lastSeen = e.getKey();
            if (numSeen >= topNumber) {
                break;
            }
        }
        return lastSeen;
    }

}
