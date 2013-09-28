package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.P4State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * Graph clean pattern: P4(Smart-algorithm) for path merge
 * 
 * @author anbangx
 */
public class P4ForPathMergeVertex extends BasicPathMergeVertex<VertexValueWritable, PathMergeMessageWritable> {

    private static final Logger LOG = Logger.getLogger(P4ForPathMergeVertex.class.getName());

    private static long randSeed = 1; //static for save memory
    private float probBeingRandomHead = -1;
    private Random randGenerator = null;

    private VKmerBytesWritable curKmer = new VKmerBytesWritable();
    private VKmerBytesWritable nextKmer = new VKmerBytesWritable();
    private VKmerBytesWritable prevKmer = new VKmerBytesWritable();
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
            outgoingMsg = new PathMergeMessageWritable();
        else
            outgoingMsg.reset();
        randSeed = Long.parseLong(getContext().getConfiguration().get(GenomixJobConf.PATHMERGE_RANDOM_RANDSEED)); // also can use getSuperstep(), because it is better to debug under deterministically random
        if (randGenerator == null)
            randGenerator = new Random(randSeed);
        if (probBeingRandomHead < 0)
            probBeingRandomHead = Float.parseFloat(getContext().getConfiguration().get(
                    GenomixJobConf.PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD));
        // Node may be marked as head b/c it's a real head or a real tail
        if (repeatKmer == null)
            repeatKmer = new VKmerBytesWritable();
        tmpValue.reset();
        if (getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
        //        else
        //            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }

    protected boolean isNodeRandomHead(VKmerBytesWritable nodeKmer) {
        // "deterministically random", based on node id
        randGenerator.setSeed((randSeed ^ nodeKmer.hashCode()) * 10000 * getSuperstep());//randSeed + nodeID.hashCode()
        for (int i = 0; i < 500; i++)  // destroy initial correlation between similar seeds 
            randGenerator.nextFloat();
        boolean isHead = randGenerator.nextFloat() < probBeingRandomHead;
        return isHead;
    }
    
    /**
     * set state as no_merge
     */
    public void setMerge(byte mergeState){
        short state = getVertexValue().getState();
        state &= P4State.MERGE_CLEAR;
        state |= (mergeState & P4State.MERGE_MASK);
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
            nextKmer = vertex.getEdgeList(nextEdgetype).get(0).getKey();
            nextHead = isNodeRandomHead(nextKmer);
        }

        // REVERSE restricted by neighbor or by my edges? 
        if (restrictedDirs.contains(DIR.REVERSE) || vertex.inDegree() != 1) {
            hasPrev = false;
        } else {
            hasPrev = true;
            prevEdgetype = vertex.getNeighborEdgeType(DIR.REVERSE); //vertex.getEdgeList(EDGETYPE.RF).getCountOfPosition() > 0 ? EDGETYPE.RF : EDGETYPE.RR; 
            prevKmer = vertex.getEdgeList(prevEdgetype).get(0).getKey();
            prevHead = isNodeRandomHead(prevKmer);
        }
    }

    public void chooseMergeDir() {
        //initiate merge_dir
        setMerge(P4State.NO_MERGE);

        curKmer = getVertexId();
        curHead = isNodeRandomHead(curKmer);
        checkNeighbors();

        if (!hasNext && !hasPrev) { // TODO check if logic for previous updates is the same as here (just look at internal flags?)
            voteToHalt(); // this node can never merge (restricted by neighbors or my structure)
        } else {
            if (curHead) {
                if (hasNext && !nextHead) {
                    // compress this head to the forward tail
                    setMerge((byte) (nextEdgetype.get() | P4State.MERGE));
                } else if (hasPrev && !prevHead) {
                    // compress this head to the reverse tail
                    setMerge((byte) (prevEdgetype.get() | P4State.MERGE));
                }
            } else {
                // I'm a tail
                if (hasNext && hasPrev) {
                    if ((!nextHead && !prevHead)
                            && (curKmer.compareTo(nextKmer) < 0 && curKmer.compareTo(prevKmer) < 0)) {
                        // tails on both sides, and I'm the "local minimum"
                        // compress me towards the tail in forward dir
                        setMerge((byte) (nextEdgetype.get() | P4State.MERGE));
                    }
                } else if (!hasPrev) {
                    // no previous node
                    if (!nextHead && curKmer.compareTo(nextKmer) < 0) {
                        // merge towards tail in forward dir
                        setMerge((byte) (nextEdgetype.get() | P4State.MERGE));
                    }
                } else if (!hasNext) {
                    // no next node
                    if (!prevHead && curKmer.compareTo(prevKmer) < 0) {
                        // merge towards tail in reverse dir
                        setMerge((byte) (prevEdgetype.get() | P4State.MERGE));
                    }
                }
            }
        }
        if (verbose) {
            LOG.fine("Iteration " + getSuperstep() + "\r\n"
            		+ "Mark: Merge from " + getVertexId() + " towards " + (EDGETYPE.fromByte(getVertexValue().getState()))
                    + "; node is " + getVertexValue());
        }
    }

    /**
     * for P4
     */
    @Override
    public void sendMergeMsg(){
        super.sendMergeMsg();
        if ((getVertexValue().getState() & P4State.MERGE) != 0) {
            deleteVertex(getVertexId());
            if (verbose) 
                LOG.fine("killing self: " + getVertexId());
        }
    }
    
    /**
     * step4: receive and process Merges
     */
    public void receiveMerges(Iterator<PathMergeMessageWritable> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        NodeWritable node = vertex.getNode();
        short state = vertex.getState();
        boolean updated = false;
        EDGETYPE senderEdgetype;
        @SuppressWarnings("unused")
        int numMerged = 0;
        while (msgIterator.hasNext()) {
            PathMergeMessageWritable incomingMsg = msgIterator.next();
            if (verbose)
                LOG.fine("Iteration " + getSuperstep() + "\r\n" 
                        + "before merge: " + getVertexValue() + " restrictions: " + DIR.enumSetFromByte(state));
            senderEdgetype = EDGETYPE.fromByte(incomingMsg.getFlag());
            node.mergeWithNode(senderEdgetype, incomingMsg.getNode());
            state |= (byte) (incomingMsg.getFlag() & DIR.MASK); // update incoming restricted directions
            numMerged++;
            updated = true;
            if (verbose)
                LOG.fine("after merge: " + getVertexValue() + " restrictions: " + DIR.enumSetFromByte(state));
        }
        if (isTandemRepeat(getVertexValue())) {
            // tandem repeats can't merge anymore; restrict all future merges
            state |= DIR.FORWARD.get();
            state |= DIR.REVERSE.get();
            updated = true;
            if (verbose)
                LOG.fine("recieveMerges is a tandem repeat: " + getVertexId() + " " + getVertexValue());
            //          updateStatisticsCounter(StatisticsCounter.Num_Cycles); 
        }
        //      updateStatisticsCounter(StatisticsCounter.Num_MergedNodes);
        //      getVertexValue().setCounters(counters);
        if (updated) {
            vertex.setState(state);
            if (DIR.enumSetFromByte(state).containsAll(EnumSet.allOf(DIR.class)))
                voteToHalt();
            else
                activate();
        }
    }

    @Override
    public void compute(Iterator<PathMergeMessageWritable> msgIterator) throws HyracksDataException {
        initVertex();
        if (getSuperstep() > maxIteration) { // TODO should we make sure the graph is complete or allow interruptions that will cause an asymmetric graph?
        	voteToHalt();
        	return;
        }

        if (getSuperstep() == 1) {
            restrictNeighbors();
        } else if (getSuperstep() % 2 == 0) {
            if (getSuperstep() == 2)
                recieveRestrictions(msgIterator);
            else
                receiveMerges(msgIterator);
            chooseMergeDir();
            updateNeighbors();
        } else if (getSuperstep() % 2 == 1) {
            receiveUpdates(msgIterator);
            sendMergeMsg();
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, P4ForPathMergeVertex.class));
    }
}
