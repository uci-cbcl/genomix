package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessage;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.util.HashedSeedRandom;

/**
 * Graph clean pattern: P4(Smart-algorithm) for path merge
 */
public class P4ForPathMergeVertex extends BasicPathMergeVertex<VertexValueWritable, PathMergeMessage> {

    private static final Logger LOG = Logger.getLogger(P4ForPathMergeVertex.class.getName());

    private static long RANDOM_SEED = -1; //static for save memory
    private float probBeingRandomHead = -1;
    private Random randGenerator = null;

    private VKmer curKmer = new VKmer();
    private VKmer nextKmer = new VKmer();
    private VKmer prevKmer = new VKmer();
    private boolean hasNext = false;
    private boolean hasPrev = false;
    private boolean curHead = false;
    private boolean nextHead = false;
    private boolean prevHead = false;
    private EDGETYPE nextEdgetype;
    private EDGETYPE prevEdgetype;

    /**
     * initiate kmerSize, maxIteration
     */

    @Override
    public void initVertex() {
        super.initVertex();
        if (outgoingMsg == null)
            outgoingMsg = new PathMergeMessage();
        else
            outgoingMsg.reset();
        if (RANDOM_SEED < 0)
            RANDOM_SEED = Long.parseLong(getContext().getConfiguration().get(GenomixJobConf.RANDOM_SEED)); // also can use getSuperstep(), because it is better to debug under deterministically random
        if (randGenerator == null) {
            randGenerator = new HashedSeedRandom();
        }
        if (probBeingRandomHead < 0)
            probBeingRandomHead = Float.parseFloat(getContext().getConfiguration().get(
                    GenomixJobConf.PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD));
        // Node may be marked as head b/c it's a real head or a real tail
        if (repeatKmer == null)
            repeatKmer = new VKmer();
        tmpValue.reset();
    }

    protected boolean isNodeRandomHead(VKmer nodeKmer) {
        // "deterministically random", based on node id
        randGenerator.setSeed((RANDOM_SEED ^ nodeKmer.hashCode()) + getSuperstep());
        boolean isHead = randGenerator.nextFloat() < probBeingRandomHead;
        return isHead;
    }

    /**
     * set state as no_merge
     */
    public void setMerge(byte mergeState) {
        short state = getVertexValue().getState();
        state &= State.MERGE_CLEAR;
        state |= (mergeState & State.MERGE_MASK);
        getVertexValue().setState(state);
        activate();
    }

    /**
     * checks if there is a valid, mergeable neighbor in the given direction. sets hasNext/Prev, next/prevEdgetype, Kmer and Head
     */
    protected void checkNeighbors() {
        VertexValueWritable vertex = getVertexValue();
        EnumSet<DIR> restrictedDirs = DIR.enumSetFromByte(vertex.getState());
        // FORWARD restricted by neighbor or by my edges? 
        if (restrictedDirs.contains(DIR.FORWARD) || vertex.outDegree() != 1) { // TODO should I restrict based on degree in the first iteration?
            hasNext = false;
        } else {
            hasNext = true;
            nextEdgetype = vertex.getNeighborEdgeType(DIR.FORWARD); //getEdgeList(EDGETYPE.FF).getCountOfPosition() > 0 ? EDGETYPE.FF : EDGETYPE.FR; 
            nextKmer = vertex.getEdgeMap(nextEdgetype).firstKey();
            nextHead = isNodeRandomHead(nextKmer);
        }

        // REVERSE restricted by neighbor or by my edges? 
        if (restrictedDirs.contains(DIR.REVERSE) || vertex.inDegree() != 1) {
            hasPrev = false;
        } else {
            hasPrev = true;
            prevEdgetype = vertex.getNeighborEdgeType(DIR.REVERSE); //vertex.getEdgeList(EDGETYPE.RF).getCountOfPosition() > 0 ? EDGETYPE.RF : EDGETYPE.RR; 
            prevKmer = vertex.getEdgeMap(prevEdgetype).firstKey();
            prevHead = isNodeRandomHead(prevKmer);
        }
    }

    public void chooseMergeDir() {
        //initiate merge_dir
        setMerge(State.NO_MERGE);

        curKmer = getVertexId();
        curHead = isNodeRandomHead(curKmer);
        checkNeighbors();
        if (verbose) {
            LOG.fine("choosing mergeDir: \ncurKmer: " + curKmer + "  curHead: " + curHead + "\nprevKmer: " + prevKmer
                    + "  prevHead: " + prevHead + "\nnextKmer: " + nextKmer + "  nextHead: " + nextHead);
        }

        if (!hasNext && !hasPrev) { // TODO check if logic for previous updates is the same as here (just look at internal flags?)
            voteToHalt(); // this node can never merge (restricted by neighbors or my structure)
        } else {
            if (curHead) {
                if (hasNext && !nextHead) {
                    // compress this head to the forward tail
                    setMerge((byte) (nextEdgetype.get() | State.MERGE));
                } else if (hasPrev && !prevHead) {
                    // compress this head to the reverse tail
                    setMerge((byte) (prevEdgetype.get() | State.MERGE));
                }
            } else {
                // I'm a tail
                if (hasNext && hasPrev) {
                    if ((!nextHead && !prevHead)
                            && (curKmer.compareTo(nextKmer) < 0 && curKmer.compareTo(prevKmer) < 0)) {
                        // tails on both sides, and I'm the "local minimum"
                        // compress me towards the tail in forward dir
                        setMerge((byte) (nextEdgetype.get() | State.MERGE));
                    }
                } else if (!hasPrev) {
                    // no previous node
                    if (!nextHead && curKmer.compareTo(nextKmer) < 0) {
                        // merge towards tail in forward dir
                        setMerge((byte) (nextEdgetype.get() | State.MERGE));
                    }
                } else if (!hasNext) {
                    // no next node
                    if (!prevHead && curKmer.compareTo(prevKmer) < 0) {
                        // merge towards tail in reverse dir
                        setMerge((byte) (prevEdgetype.get() | State.MERGE));
                    }
                }
            }
        }
        if (verbose) {
            if ((getVertexValue().getState() & State.MERGE) != 0) {
                LOG.fine("Mark: Merge from " + getVertexId() + " towards "
                        + (EDGETYPE.fromByte(getVertexValue().getState())) + "; node is " + getVertexValue());
            } else {
                LOG.fine("Mark: No Merge for " + getVertexId() + " node is " + getVertexValue());
            }
        }
    }

    /**
     * for P4
     */
    @Override
    public void sendMergeMsg() {
        if (verbose) {
            LOG.fine("Checking if I should send a merge message..." + getVertexValue());
        }
        super.sendMergeMsg();
        if ((getVertexValue().getState() & State.MERGE) != 0) {
            deleteVertex(getVertexId());
            if (verbose)
                LOG.fine("killing self: " + getVertexId());
        }
    }

    /**
     * step4: receive and process Merges
     */
    public void receiveMerges(Iterator<PathMergeMessage> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        Node node = vertex;
        short state = vertex.getState();
        boolean updated = false;
        EDGETYPE senderEdgetype;
        //        int numMerged = 0;
        while (msgIterator.hasNext()) {
            PathMergeMessage incomingMsg = msgIterator.next();
            if (verbose) {
                LOG.fine("before merge: " + getVertexValue() + " restrictions: " + DIR.enumSetFromByte(state));
            }
            senderEdgetype = EDGETYPE.fromByte(incomingMsg.getFlag());
            node.mergeWithNodeUsingTruncatedKmer(senderEdgetype, incomingMsg.getNode());
            state |= (byte) (incomingMsg.getFlag() & DIR.MASK); // update incoming restricted directions
            //            numMerged++;
            updated = true;
            if (verbose) {
                LOG.fine("after merge: " + getVertexValue() + " restrictions: " + DIR.enumSetFromByte(state));
            }
        }
        if (isTandemRepeat(getVertexValue())) {
            // tandem repeats can't merge anymore; restrict all future merges
            state |= DIR.FORWARD.get();
            state |= DIR.REVERSE.get();
            updated = true;
            if (verbose) {
                LOG.fine("recieveMerges is a tandem repeat: " + getVertexId() + " " + getVertexValue());
            }
            //          updateStatisticsCounter(StatisticsCounter.Num_Cycles); 
        }
        //      updateStatisticsCounter(StatisticsCounter.Num_MergedNodes);
        //      getVertexValue().setCounters(counters);
        if (updated) {
            vertex.setState(state);
            if (DIR.enumSetFromByte(state).containsAll(Arrays.asList(DIR.values())))
                voteToHalt();
            else
                activate();
        }
    }

    @Override
    public void compute(Iterator<PathMergeMessage> msgIterator) {
        initVertex();
        if (Float.isInfinite(getVertexValue().getAverageCoverage()) || Float.isNaN(getVertexValue().getAverageCoverage())) {
            System.out.println("Before: " + getVertexValue());
        }
        if (getVertexId().toString().equals("AGCGCAAGG")) {
            System.out.println();
        }
        if (verbose)
            LOG.fine("Iteration " + getSuperstep() + " for key " + getVertexId());
        if (getSuperstep() > maxIteration) { // TODO should we make sure the graph is complete or allow interruptions that will cause an asymmetric graph?
            voteToHalt();
            return;
        }

        if (getSuperstep() == 1) {
            restrictNeighbors();
        } else if (getSuperstep() % 2 == 0) {
            if (getSuperstep() == 2) {
                recieveRestrictions(msgIterator);
            } else {
                receiveMerges(msgIterator);
            }
            chooseMergeDir();
            updateNeighbors();
        } else if (getSuperstep() % 2 == 1) {
            receiveUpdates(msgIterator);
            sendMergeMsg();
        }
        if (Float.isInfinite(getVertexValue().getAverageCoverage()) || Float.isNaN(getVertexValue().getAverageCoverage())) {
            System.out.println("after: " + getVertexValue());
            throw new RuntimeException(this.toString());
        }
    }

}
