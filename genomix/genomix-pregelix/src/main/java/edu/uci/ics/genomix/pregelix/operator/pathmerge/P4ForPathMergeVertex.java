package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.jfree.util.Log;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.P4State;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;
import edu.uci.ics.genomix.type.NodeWritable.IncomingListFlag;
import edu.uci.ics.genomix.type.NodeWritable.OutgoingListFlag;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * Graph clean pattern: P4(Smart-algorithm) for path merge 
 * @author anbangx
 *
 */
public class P4ForPathMergeVertex extends
    BasicPathMergeVertex<VertexValueWritable, PathMergeMessageWritable> {
    
    private static final Logger LOG = Logger.getLogger(P4ForPathMergeVertex.class.getName());
    
    private static long randSeed = 1; //static for save memory
    private float probBeingRandomHead = -1;
    private Random randGenerator = null;
    
    private VKmerBytesWritable curKmer = new VKmerBytesWritable();
    private VKmerBytesWritable nextKmer = new VKmerBytesWritable();
    private VKmerBytesWritable prevKmer = new VKmerBytesWritable();
    private boolean hasNext;
    private boolean hasPrev;
    private boolean curHead;
    private boolean nextHead;
    private boolean prevHead;
    private byte nextDir;
    private byte prevDir;
    
    private static final List<VKmerBytesWritable> problemKmers = Arrays.asList(
            new VKmerBytesWritable("CCCGGCCTCCAGCGTGGGATACGCGAAGATGCCGCCGTAGGTGAGAATCTGGTTC"),
            new VKmerBytesWritable("GCAGGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            new VKmerBytesWritable("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            new VKmerBytesWritable("GAGCAGGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            new VKmerBytesWritable("GGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            new VKmerBytesWritable("AGGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            new VKmerBytesWritable("GCGACGTGCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            new VKmerBytesWritable("GTCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
            );
    private boolean verbose;
    
    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if(incomingMsg == null)
            incomingMsg = new PathMergeMessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new PathMergeMessageWritable();
        else
            outgoingMsg.reset();
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        randSeed = Long.parseLong(getContext().getConfiguration().get(GenomixJobConf.PATHMERGE_RANDOM_RANDSEED)); // also can use getSuperstep(), because it is better to debug under deterministically random
        if(randGenerator == null)
            randGenerator = new Random(randSeed); 
        if (probBeingRandomHead < 0)
            probBeingRandomHead = Float.parseFloat(getContext().getConfiguration().get(GenomixJobConf.PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD));
        hasNext = false;
        hasPrev = false;
        curHead = false;
        nextHead = false;
        prevHead = false;
        outFlag = (byte)0;
        inFlag = (byte)0;
        // Node may be marked as head b/c it's a real head or a real tail
        if(repeatKmer == null)
            repeatKmer = new VKmerBytesWritable();
        tmpValue.reset();
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
//        else
//            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();

        verbose = false;
        for (VKmerBytesWritable pk : problemKmers)
            verbose |= getVertexValue().getNode().findEdge(pk) != null || getVertexId().equals(pk);
        if (verbose)
            LOG.fine("iteration " + getSuperstep());
    }
    
    /**
     * Send merge restrictions to my neighbor nodes
     */
    public void restrictNeighbors() {
        EnumSet<DIR> dirsToRestrict;
        VertexValueWritable vertex = getVertexValue();
        short state = vertex.getState();
        boolean updated = false;
        if(isTandemRepeat(vertex)) {
            // tandem repeats are not allowed to merge at all
            dirsToRestrict = EnumSet.of(DIR.NEXT, DIR.PREVIOUS);
            state |= DIR.NEXT.get();
            state |= DIR.PREVIOUS.get();
            updated = true;
        }
        else {
            // degree > 1 can't merge in that direction; == 0 means we are a tip 
            dirsToRestrict = EnumSet.noneOf(DIR.class);
            for (DIR dir : DIR.values()) {
                if (vertex.getDegree(dir) > 1 || vertex.getDegree(dir) == 0) {
                    dirsToRestrict.add(dir);
                    state |= dir.get();
                    updated = true;
                }
            }
        }
        if (updated) {
            vertex.setState(state);
            if (DIR.enumSetFromByte(state).containsAll(EnumSet.allOf(DIR.class)))
            	voteToHalt();
            else 
            	activate();
        }
        
        // send a message to each neighbor indicating they can't merge towards me
        for (DIR dir : dirsToRestrict) {
            for (byte d : NodeWritable.edgeTypesInDir(dir)) {
                for (VKmerBytesWritable destId : vertex.getEdgeList(d).getKeys()) {
                    for (VKmerBytesWritable pk : problemKmers)
                        verbose |= destId.equals(pk);
                    outgoingMsg.reset();
                    outgoingMsg.setFlag(DirectionFlag.dirFromEdgeType(DirectionFlag.mirrorEdge(d)).get());
                    if (verbose)
                        LOG.fine("send restriction from " + getVertexId() + " to " + destId + " in my " + d + " and their " + DirectionFlag.mirrorEdge(d) + " (" + DirectionFlag.dirFromEdgeType(DirectionFlag.mirrorEdge(d)) + "); I am " + getVertexValue());
                    sendMsg(destId, outgoingMsg);
                }
            }
        }
    }
    
    /**
     * initiate head, rear and path node
     */
    public void recieveRestrictions(Iterator<PathMergeMessageWritable> msgIterator) {
        short restrictedDirs = 0;  // the directions (NEXT/PREVIOUS) that I'm not allowed to merge in
        boolean updated = false;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            for (VKmerBytesWritable pk : problemKmers)
                verbose |= incomingMsg.getNode().findEdge(pk) != null || incomingMsg.getSourceVertexId().equals(pk);
            if (verbose)
                LOG.fine("before restriction " + getVertexId() + ": " + DIR.fromByte(restrictedDirs));
            restrictedDirs |= incomingMsg.getFlag();
            if (verbose)
                LOG.fine("after restriction " + getVertexId() + ": " + DIR.fromByte(restrictedDirs));
            updated = true;
        }
        if (updated) {
            getVertexValue().setState(restrictedDirs);
            if (DIR.enumSetFromByte(restrictedDirs).containsAll(EnumSet.allOf(DIR.class)))
                voteToHalt();
            else 
                activate();
        }
    }

    protected boolean isNodeRandomHead(VKmerBytesWritable nodeKmer) {
        // "deterministically random", based on node id
        randGenerator.setSeed((randSeed ^ nodeKmer.hashCode()) * 10000 * getSuperstep());//randSeed + nodeID.hashCode()
        for(int i = 0; i < 500; i++)
            randGenerator.nextFloat();
        boolean isHead = randGenerator.nextFloat() < probBeingRandomHead;
        if (verbose)
            LOG.fine("randomHead: " + nodeKmer + "=" + isHead);
        return isHead;
    }
    
    /**
     * checks if there is a valid, mergeable neighbor in the given direction.  sets hasNext/Prev, next/prevDir, Kmer and Head
     */
    protected void checkNeighbors() {
        VertexValueWritable vertex = getVertexValue();
        EnumSet<DIR> restrictedDirs = DIR.enumSetFromByte(vertex.getState());
        // NEXT restricted by neighbor or by my edges? 
        if (restrictedDirs.contains(DIR.NEXT) || vertex.outDegree() != 1) { // TODO should I restrict based on degree in the first iteration?
            hasNext = false;
        } else {
            hasNext = true;
            nextDir = vertex.getEdgeList(DirectionFlag.DIR_FF).getCountOfPosition() > 0 ? DirectionFlag.DIR_FF : DirectionFlag.DIR_FR; 
            nextKmer = vertex.getEdgeList(nextDir).get(0).getKey();
            nextHead = isNodeRandomHead(nextKmer);
        }

        // PREVIOUS restricted by neighbor or by my edges? 
        if (restrictedDirs.contains(DIR.PREVIOUS) || vertex.inDegree() != 1) {
            hasPrev = false;
        } else {
            hasPrev = true;
            prevDir = vertex.getEdgeList(DirectionFlag.DIR_RF).getCountOfPosition() > 0 ? DirectionFlag.DIR_RF : DirectionFlag.DIR_RR; 
            prevKmer = vertex.getEdgeList(prevDir).get(0).getKey();
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
            voteToHalt();  // this node can never merge (restricted by neighbors or my structure)
        } else {
            if (curHead) {
                if (hasNext && !nextHead) {
                    // compress this head to the forward tail
                    setMerge((byte) (nextDir | P4State.MERGE));
                } else if (hasPrev && !prevHead) {
                    // compress this head to the reverse tail
                    setMerge((byte) (prevDir | P4State.MERGE));
                } 
            }
            else {
                // I'm a tail
                if (hasNext && hasPrev) {
                     if ((!nextHead && !prevHead) && (curKmer.compareTo(nextKmer) < 0 && curKmer.compareTo(prevKmer) < 0)) {
                        // tails on both sides, and I'm the "local minimum"
                        // compress me towards the tail in forward dir
                        setMerge((byte) (nextDir | P4State.MERGE));
                    }
                } else if (!hasPrev) {
                    // no previous node
                    if (!nextHead && curKmer.compareTo(nextKmer) < 0) {
                        // merge towards tail in forward dir
                        setMerge((byte) (nextDir | P4State.MERGE));
                    }
                } else if (!hasNext) {
                    // no next node
                    if (!prevHead && curKmer.compareTo(prevKmer) < 0) {
                        // merge towards tail in reverse dir
                        setMerge((byte) (prevDir | P4State.MERGE));
                    }
                }
            }
        }
        if (verbose) {
            if ((getVertexValue().getState() & P4State.MERGE) == 0) {
                LOG.fine("No merge for " + getVertexId());
            } else {
                LOG.fine("Merge from " + getVertexId() + " towards " + (getVertexValue().getState() & DirectionFlag.DIR_MASK) + "; node is " + getVertexValue());
            }
        }
    }
    
    public void updateNeighbors() {
        VertexValueWritable vertex = getVertexValue();
        short state = vertex.getState();
        if ((state & P4State.MERGE) == 0) {
            return;  // no merge requested; don't have to update neighbors
        }
        
        DIR mergeDir = DirectionFlag.dirFromEdgeType((byte)state);
        byte[] mergeEdges = NodeWritable.edgeTypesInDir(mergeDir);
        
        DIR updateDir = mergeDir.mirror();
        byte[] updateEdges = NodeWritable.edgeTypesInDir(updateDir); // 
        
        // prepare the update message s.t. the receiver can do a simple unionupdate
        // that means we figure out any hops and place our merge-dir edges in the appropriate list of the outgoing msg
        for (byte updateEdge : updateEdges) {
            outgoingMsg.reset();
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setFlag(DirectionFlag.mirrorEdge(updateEdge));  // neighbor's edge to me (so he can remove me) 
            for (byte mergeEdge : mergeEdges) {
                byte newDir = DirectionFlag.resolveLinkThroughMiddleNode(updateEdge, mergeEdge);
                outgoingMsg.getNode().setEdgeList(newDir, getVertexValue().getEdgeList(mergeEdge));  // copy into outgoingMsg
            }
            for (VKmerBytesWritable pk : problemKmers)
                verbose |= outgoingMsg.getNode().findEdge(pk) != null;
            
            // send the update to all kmers in this list // TODO perhaps we could skip all this if there are no neighbors here
            for (VKmerBytesWritable dest : vertex.getEdgeList(updateEdge).getKeys()) {
                if (verbose)
                    LOG.fine("send update message from " + getVertexId() + " to " + dest + ": " + outgoingMsg);
                sendMsg(dest, outgoingMsg);
            }
        }
    }
    
    public void receiveUpdates(Iterator<PathMergeMessageWritable> msgIterator) throws HyracksDataException{
        VertexValueWritable vertex = getVertexValue(); 
        NodeWritable node = vertex.getNode();
        boolean updated = false;
        ArrayList<PathMergeMessageWritable> allSeenMsgs = new ArrayList<PathMergeMessageWritable>();
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            for (VKmerBytesWritable pk : problemKmers)
                verbose |= incomingMsg.getNode().findEdge(pk) != null || incomingMsg.getSourceVertexId().equals(pk);
            if (verbose)
                LOG.fine("before update from neighbor: " + getVertexValue());
            // remove the edge to the node that will merge elsewhere
            try {
                node.getEdgeList((byte)(incomingMsg.getFlag() & DirectionFlag.DIR_MASK)).remove(incomingMsg.getSourceVertexId());
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new HyracksDataException("In update, tried to remove an edge that doesn't exist...\nvertex: " + vertex + "\nremoving " + incomingMsg.getSourceVertexId() + " from dir " + (incomingMsg.getFlag() & DirectionFlag.DIR_MASK) + "\nupdate node: " + incomingMsg.getNode() + "\npreviously recieved messages this iteration: \n{\n" + StringUtils.join(allSeenMsgs, "\n") + "\n}\n", e);
            }
            // add the node this neighbor will merge into
            for (byte dir : DirectionFlag.values) {
                node.getEdgeList(dir).unionUpdate(incomingMsg.getEdgeList(dir));
            }
            updated = true;
            allSeenMsgs.add(incomingMsg);
            if (verbose) 
                LOG.fine("after update from neighbor: " + getVertexValue());
        }
        if (verbose)
            LOG.fine("All recieved updates:  \n{\n" + StringUtils.join(allSeenMsgs, "\n") + "\n}\n");
        if (updated) {
            if (DIR.enumSetFromByte(vertex.getState()).containsAll(EnumSet.allOf(DIR.class)))
                voteToHalt();
            else 
                activate();
        }
            
//            checkNeighbors();
//            if (!hasNext && !hasPrev)
//                voteToHalt();
//            else
//                activate();
    }
    
    public void broadcastMerge() {
        VertexValueWritable vertex = getVertexValue();
        short state = vertex.getState();
        if ((state & P4State.MERGE) != 0) {
            outgoingMsg.reset();
            // tell neighbor where this is coming from (so they can merge kmers and delete)
            byte mergeDir = (byte)(vertex.getState() & DirectionFlag.DIR_MASK);
            byte neighborRestrictions = DIR.fromSet(DirectionFlag.causesFlip(mergeDir) ? DIR.flipSetFromByte(state) : DIR.enumSetFromByte(state));
            
            outgoingMsg.setFlag((short) (DirectionFlag.mirrorEdge(mergeDir) | neighborRestrictions));
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setNode(vertex.getNode());
            if (vertex.getDegree(DirectionFlag.dirFromEdgeType(mergeDir)) != 1)
                throw new IllegalStateException("Merge attempted in node with degree in " + mergeDir + " direction != 1!\n" + vertex);
            VKmerBytesWritable dest = vertex.getEdgeList(mergeDir).get(0).getKey();
            for (VKmerBytesWritable pk : problemKmers)
                verbose |= outgoingMsg.getNode().findEdge(pk) != null || dest.equals(pk);
            if (verbose)
                LOG.fine("send merge mesage from " + getVertexId() + " to " + dest + ": " + outgoingMsg + "; my restrictions are: " + DIR.enumSetFromByte(vertex.getState()) + ", their restrictions are: " + DIR.enumSetFromByte(outgoingMsg.getFlag()));
            sendMsg(dest, outgoingMsg);
            
            if (verbose)
                LOG.fine("killing self: " + getVertexId());
            deleteVertex(getVertexId());
        }
    }
    
    /**
     * step4: processMerges 
     */
    public void receiveMerges(Iterator<PathMergeMessageWritable> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        NodeWritable node = vertex.getNode();
        short state = vertex.getState();
        boolean updated = false;
        byte senderDir;
        int numMerged = 0;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            for (VKmerBytesWritable pk : problemKmers)
                verbose |= incomingMsg.getNode().findEdge(pk) != null;
            if (verbose)
                LOG.fine("before merge: " + getVertexValue() + " restrictions: " + DIR.enumSetFromByte(state));
            senderDir = (byte) (incomingMsg.getFlag() & DirectionFlag.DIR_MASK);
            node.mergeWithNode(senderDir, incomingMsg.getNode());
            state |= (byte) (incomingMsg.getFlag() & DIR.MASK);  // update incoming restricted directions
            numMerged++;
            updated = true;
            if (verbose)
                LOG.fine("after merge: " + getVertexValue() + " restrictions: " + DIR.enumSetFromByte(state));
        }
        if(isTandemRepeat(getVertexValue())) {
            // tandem repeats can't merge anymore; restrict all future merges
            state |= DIR.NEXT.get();
            state |= DIR.PREVIOUS.get();
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
            broadcastMerge();
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, P4ForPathMergeVertex.class));
    }
}
