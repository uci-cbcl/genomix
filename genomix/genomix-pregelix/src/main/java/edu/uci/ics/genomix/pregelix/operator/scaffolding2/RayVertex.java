package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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
import edu.uci.ics.genomix.data.types.Node;
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
    private int SEED_SCORE_THRESHOLD;
    private int SEED_LENGTH_THRESHOLD;
    private int COVERAGE_DIST_NORMAL_MEAN;
    private int COVERAGE_DIST_NORMAL_STD;
    private static boolean HAS_PAIRED_END_READS;
    private static int MAX_READ_LENGTH;
    private static int MAX_OUTER_DISTANCE;
    private static int MIN_OUTER_DISTANCE;
    private static int MAX_DISTANCE; // the max(readlengths, outerdistances)

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
        SEED_SCORE_THRESHOLD = Integer.parseInt(conf.get(GenomixJobConf.SCAFFOLDING_SEED_SCORE_THRESHOLD));
        SEED_LENGTH_THRESHOLD = Integer.parseInt(conf.get(GenomixJobConf.SCAFFOLDING_SEED_LENGTH_THRESHOLD));
        
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
            getVertexValue().flippedFromInitialDirection = false;
            getVertexValue().stopSearch = false;
        }
    }

    @Override
    public void compute(Iterator<RayMessage> msgIterator) throws Exception {
        if (getSuperstep() == 1) {
            if (isStartSeed()) {
                msgIterator = getStartMessage();
            }
        }
        scaffold(msgIterator);
        voteToHalt();
    }

    /**
     * @return whether or not this node meets the "seed" criteria
     */
    private boolean isStartSeed() {
        float coverage = getVertexValue().getAverageCoverage(); 
        return ((coverage >= COVERAGE_DIST_NORMAL_MEAN - COVERAGE_DIST_NORMAL_STD) 
                && (coverage <= COVERAGE_DIST_NORMAL_MEAN + COVERAGE_DIST_NORMAL_STD)
                && (getVertexValue().calculateSeedScore() >= SEED_SCORE_THRESHOLD));
//        return getVertexValue().getKmerLength() >= MIN_SCAFFOLDING_SEED_LENGTH;
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
                case REQUEST_KMER:
                    sendKmerToWalkNodes(msg);
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
                    vertex.getEdges(msg.getEdgeTypeBackToFrontier()).remove(msg.getSourceVertexId());
                    vertex.visited = false;
                    break;
                case STOP:
                    // I am a frontier node but one of my neighbors was already included in a different walk.
                    // that neighbor is marked "intersection" and I need to remember the stopped state.  No path
                    // can continue out of me in the future, even when I receive aggregate scores
                    vertex.stopSearch = true;
                    // TODO obey this stop search in the CONTINUE branch
                    break;
            }
        }
        if (requestScoreMsgs.size() > 0) {
            sendScoresToFrontier(requestScoreMsgs);
        }
        if (aggregateScoreMsgs.size() > 0) {
            compareScoresAndPrune(aggregateScoreMsgs);
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
            LOG.info("start branch comparison had to stop at " + id);
            return;
        }
        vertex.visited = true;
        // I am the new frontier but this message is coming from the previous frontier; I was the "candidate"
        vertex.flippedFromInitialDirection = msg.isCandidateFlipped();
        DIR nextDir = msg.getEdgeTypeBackToFrontier().mirror().neighborDir();

        msg.visitNode(id, vertex);

        if (vertex.degree(nextDir) == 0) {
            // this walk has reached a dead end!  nothing to do in this case.
            LOG.info("reached dead end: " + id);
            return;
        } else if (vertex.degree(nextDir) == 1) {
            // one neighbor -> just send him a continue msg w/ me added to the list
            NeighborInfo next = vertex.getSingleNeighbor(nextDir);
            msg.setEdgeTypeBackToFrontier(next.et.mirror());
            msg.setFrontierFlipped(vertex.flippedFromInitialDirection);
            sendMsg(next.kmer, msg);
            LOG.info("bouncing over path node: " + id);
        } else {
            // 2+ neighbors -> start evaluating candidates via a REQUEST_KMER msg
            msg.setMessageType(RayMessageType.REQUEST_KMER);
            msg.setFrontierFlipped(vertex.flippedFromInitialDirection);
            msg.setSourceVertexId(id);
            for (EDGETYPE et : nextDir.edgeTypes()) {
                for (VKmer next : vertex.getEdges(et)) {
                    msg.setEdgeTypeBackToFrontier(et.mirror());
                    sendMsg(next, msg);
                    LOG.info("evaluating branch: " + next);
                }
            }
        }
    }

    /**
     * I'm a candidate and need to send my kmer to all the nodes of the walk
     * if I've been visited already, I need to send a STOP msg back to the frontier node and
     * mark this node as an "intersection" between multiple seeds
     * 
     * @param msg
     */
    private void sendKmerToWalkNodes(RayMessage msg) {
        VKmer id = getVertexId();
        RayValue vertex = getVertexValue();

        // already visited -> the frontier must stop!
        if (vertex.visited) {
            vertex.intersection = true;
            outgoingMsg.reset();
            outgoingMsg.setMessageType(RayMessageType.STOP);
            sendMsg(msg.getSourceVertexId(), outgoingMsg);
            return;
        }

        outgoingMsg.reset();
        outgoingMsg.setMessageType(RayMessageType.REQUEST_SCORE);
        outgoingMsg.setSourceVertexId(msg.getSourceVertexId()); // frontier node
        outgoingMsg.setFrontierFlipped(msg.getFrontierFlipped());
        outgoingMsg.setEdgeTypeBackToFrontier(msg.getEdgeTypeBackToFrontier());
        outgoingMsg.setToScoreId(id); // candidate node (me)
        outgoingMsg.setToScoreKmer(vertex.getInternalKmer());
        outgoingMsg.setWalkLength(msg.getWalkLength());
        for (int i = 0; i < msg.getWalkIds().size(); i++) {
            outgoingMsg.getWalkOffsets().clear();
            outgoingMsg.getWalkOffsets().add(msg.getWalkOffsets().get(i)); // the offset of the destination node
            outgoingMsg.getWalkIds().clear();
            outgoingMsg.getWalkIds().append(msg.getWalkIds().getPosition(i));
            sendMsg(msg.getWalkIds().getPosition(i), outgoingMsg);
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
    private void sendScoresToFrontier(ArrayList<RayMessage> msgs) {
        VKmer id = getVertexId();
        RayValue vertex = getVertexValue();

        // all msgs should have the same total length and describe me as being at the same offset
        int myOffset = msgs.get(0).getWalkOffsets().get(0);
        int walkLength = msgs.get(0).getWalkLength();
        VKmer frontierNode = msgs.get(0).getSourceVertexId();

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
                sendMsg(frontierNode, outgoingMsg);
            }
            return;
        }

        // get the smallest kmer in all the messages I've received
        // since the candidates may be of different lengths, we have to use the shortest candidate
        // that way, long candidates don't receive higher scores simply for being long
        int minLength = msgs.get(0).getToScoreKmer().getKmerLetterLength();
        for (int i = 1; i < msgs.size(); i++) {
            minLength = Math.min(minLength, msgs.get(i).getToScoreKmer().getKmerLetterLength());
        }
        minLength = minLength - Kmer.getKmerLength() + 1;

        // I'm now allowed to score the first minLength kmers according to my readids
        RayScores singleEndScores = voteFromReads(true, vertex, msgs, myOffset, walkLength, minLength);
        RayScores pairedEndScores = HAS_PAIRED_END_READS ? voteFromReads(false, vertex, msgs, myOffset, walkLength,
                minLength) : null;

        outgoingMsg.reset();
        outgoingMsg.setMessageType(RayMessageType.AGGREGATE_SCORE);
        outgoingMsg.setWalkLength(walkLength);
        outgoingMsg.getWalkOffsets().add(myOffset);
        outgoingMsg.getWalkIds().append(id);
        outgoingMsg.setSingleEndScores(singleEndScores);
        outgoingMsg.setPairedEndScores(pairedEndScores);
        sendMsg(frontierNode, outgoingMsg);
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
    private static RayScores voteFromReads(boolean singleEnd, RayValue vertex, ArrayList<RayMessage> candidateMsgs,
            int nodeOffset, int walkLength, int msgKmerLength) {
        SortedSet<ReadHeadInfo> readSubsetOrientedWithSearch = getReadSubsetOrientedWithSearch(singleEnd, vertex,
                nodeOffset, walkLength);

        // nothing like nested for loops 4 levels deep (!)
        RayScores scores = new RayScores();
        for (RayMessage msg : candidateMsgs) {
            VKmer candidateKmer;
            if (singleEnd) {
                // for single-end reads, we need the candidate in the same orientation as the search
                candidateKmer = msg.isCandidateFlipped() ? msg.getToScoreKmer().reverse() : msg.getToScoreKmer();
            } else {
                // for paired-end reads, the mate sequence is revcomp'ed; need the candidate the opposite orientation as the search
                candidateKmer = msg.isCandidateFlipped() ? msg.getToScoreKmer() : msg.getToScoreKmer().reverse();
            }
            int ruleATotal = 0, ruleBTotal = 0, ruleCTotal = 0;
            for (ReadHeadInfo read : readSubsetOrientedWithSearch) {
                for (int kmerIndex = 0; kmerIndex < msgKmerLength; kmerIndex++) {
                    boolean match = false;
                    // TODO we currently keep the score separately for each kmer we're considering
                    // ruleC is about the minimum value in the comparison of the single kmers adjacent to the frontier
                    // but we're currently using it as the minimum across many kmers.  We'll have to think about this 
                    // rule some more and what it means in a merged graph

                    if (singleEnd) {
                        if (read.getThisReadSequence().matchesExactly(
                                walkLength - nodeOffset - read.getOffset() + kmerIndex, candidateKmer, kmerIndex,
                                Kmer.getKmerLength())) {
                            match = true;
                        }
                    } else {
                        int readLength = 100;
                        int outerDistanceMean = 500;
                        int outerDistanceStd = 30;
                        int mateStart = nodeOffset + read.getOffset() + outerDistanceMean - readLength;
                        int candidateInMate = walkLength - mateStart + kmerIndex;
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
                        ruleATotal += walkLength - nodeOffset - read.getOffset();
                        ruleBTotal++;
                        ruleCTotal++;
                    }
                }
            }
            scores.addRuleCounts(msg.getEdgeTypeBackToFrontier().mirror(), msg.getToScoreId(), ruleATotal, ruleBTotal,
                    ruleCTotal);
        }
        if (scores.size() > 0) {
            return scores;
        } else {
            return null;
        }
    }

    // local variables for getReadSubsetOrientedWithSearch
    @SuppressWarnings("unchecked")
    private static final SortedSet<ReadHeadInfo> EMPTY_SORTED_SET = SetUtils.EMPTY_SORTED_SET;

    private static SortedSet<ReadHeadInfo> getReadSubsetOrientedWithSearch(boolean singleEnd, RayValue vertex,
            int nodeOffset, int walkLength) {
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
        if (vertex.flippedFromInitialDirection) {
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
        return orientedReads.getOffSetRange(Math.max(0, startOffset), Math.min(myLength, endOffset));
    }

    // local variables for compareScoresAndPrune
    private RayScores singleEndScores = new RayScores();
    private RayScores pairedEndScores = new RayScores();
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
            LOG.info("prune and search had to stop at " + id);
            return;
        }

        // aggregate scores and walk info from all msgs
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
            singleEndScores.addAll(msg.getSingleEndScores());
            pairedEndScores.addAll(msg.getPairedEndScores());
            walkIds.append(msg.getWalkIds().getPosition(0));
            walkOffsets.add(msg.getWalkOffsets().get(0));
        }
        LOG.info("in prune for " + id + " scores are singleend: " + singleEndScores + " pairedend: " + pairedEndScores);

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
        int queryIndex = 0, targetIndex = 0;
        boolean equalPairedEdgeFound = false;
        boolean equalSingleEdgeFound = false;
        boolean dominantEdgeFound = false;
        EDGETYPE[] searchETs = vertex.flippedFromInitialDirection ? DIR.REVERSE.edgeTypes() : DIR.FORWARD.edgeTypes();
        for (EDGETYPE queryET : searchETs) {
            for (VKmer queryKmer : vertex.getEdges(queryET)) {
                queryIndex++;
                targetIndex = 0;
                equalPairedEdgeFound = false;
                equalSingleEdgeFound = false;
                for (EDGETYPE targetET : searchETs) {
                     for (VKmer targetKmer : vertex.getEdges(targetET)) {
                        targetIndex++;
                        if (queryIndex == targetIndex) {
                            // don't compare vs self
                            continue;
                        }
                        float coverage = vertex.getAverageCoverage();
                        if (!pairedEndScores.dominates(queryET, queryKmer, targetET, targetKmer, coverage)) {
                            equalPairedEdgeFound = true;
                        }
                        if (!singleEndScores.dominates(queryET, queryKmer, targetET, targetKmer, coverage)) {
                            equalSingleEdgeFound = true;
                        }
                    }
                    if (equalPairedEdgeFound && equalSingleEdgeFound) {
                        break;
                    }
                }
                if (!equalPairedEdgeFound) {
                    // this edge dominated all other edges.  Keep it as the winner
                    dominantKmer = queryKmer;
                    dominantEdgeType = queryET;
                    dominantEdgeFound = true;
                    break;
                } else if (!equalSingleEdgeFound) {
                    dominantKmer = queryKmer;
                    dominantEdgeType = queryET;
                    dominantEdgeFound = true;
                    break;
                }
            }
            if (dominantEdgeFound) {
                break;
            }
        }

        if (dominantEdgeFound) {
            // if a dominant edge is found, all the others must be removed.
            for (EDGETYPE et : searchETs) {
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
            // the walk is then passed on to the single remaining node
            outgoingMsg.reset();
            outgoingMsg.setMessageType(RayMessageType.CONTINUE_WALK);
            outgoingMsg.setEdgeTypeBackToFrontier(dominantEdgeType.mirror());
            outgoingMsg.setWalkIds(walkIds);
            outgoingMsg.setWalkOffsets(walkOffsets);
            outgoingMsg.setWalkLength(walkLength);
            outgoingMsg.setFrontierFlipped(vertex.flippedFromInitialDirection); // TODO make sure this is correct
            sendMsg(dominantKmer, outgoingMsg);
            LOG.info("dominant edge found: " + dominantEdgeType + ":" + dominantKmer);
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

    public Logger LOG = Logger.getLogger(RayVertex.class.getName());

    @SuppressWarnings("deprecation")
    public static int calculateScoreThreshold(Counters statsCounters, Float topFraction, Integer topNumber) {
        if ((topFraction == null && topNumber == null) || (topFraction != null && topNumber != null)) {
            throw new IllegalArgumentException("Please specify either topFraction or topNumber, but not both!");
        }
        TreeMap<Integer, Long> scoreHistogram = new TreeMap<>();
        int total = 0;
        for (Counter c : statsCounters.getGroup("scaffoldSeedScore-bins")) { // counter name is index; counter value is the count for this index
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
