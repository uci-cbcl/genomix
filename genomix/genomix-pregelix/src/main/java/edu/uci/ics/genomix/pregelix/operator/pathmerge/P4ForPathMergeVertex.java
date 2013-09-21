package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Random;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.P4State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

/**
 * Graph clean pattern: P4(Smart-algorithm) for path merge 
 * @author anbangx
 *
 */
public class P4ForPathMergeVertex extends
    BasicPathMergeVertex<VertexValueWritable, PathMergeMessageWritable> {
    
//    private static final Logger LOG = Logger.getLogger(P4ForPathMergeVertex.class.getName());
    
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
    private EDGETYPE nextEdgetype;
    private EDGETYPE prevEdgetype;
    
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
            for (EDGETYPE et : NodeWritable.edgeTypesInDir(dir)) {
                for (VKmerBytesWritable destId : vertex.getEdgeList(et).getKeys()) {
                    outgoingMsg.reset();
                    outgoingMsg.setFlag(et.mirror().dir().get());
                    
//                    LOG.info("send restriction from " + getVertexId() + " to " + destId + " in my " + d + " and their " + DirectionFlag.mirrorEdge(d) + " (" + DirectionFlag.dirFromEdgeType(DirectionFlag.mirrorEdge(d)) + "); I am " + getVertexValue());
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
//            LOG.info("before restriction " + getVertexId() + ": " + DIR.fromByte(restrictedDirs));
            incomingMsg = msgIterator.next();
            restrictedDirs |= incomingMsg.getFlag();
//            LOG.info("after restriction " + getVertexId() + ": " + DIR.fromByte(restrictedDirs));
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
//        LOG.info("randomHead: " + nodeKmer + "=" + isHead);
        return isHead;
    }
    
    /**
     * checks if there is a valid, mergeable neighbor in the given direction.  sets hasNext/Prev, next/prevEdgetype, Kmer and Head
     */
    protected void checkNeighbors() {
        VertexValueWritable vertex = getVertexValue();
        EnumSet<DIR> restrictedDirs = DIR.enumSetFromByte(vertex.getState());
        // NEXT restricted by neighbor or by my edges? 
        if (restrictedDirs.contains(DIR.NEXT) || vertex.outDegree() != 1) { // TODO should I restrict based on degree in the first iteration?
            hasNext = false;
        } else {
            hasNext = true;
            nextEdgetype = vertex.getEdgeList(EDGETYPE.FF).getCountOfPosition() > 0 ? EDGETYPE.FF : EDGETYPE.FR; 
            nextKmer = vertex.getEdgeList(nextEdgetype).get(0).getKey();
            nextHead = isNodeRandomHead(nextKmer);
        }

        // PREVIOUS restricted by neighbor or by my edges? 
        if (restrictedDirs.contains(DIR.PREVIOUS) || vertex.inDegree() != 1) {
            hasPrev = false;
        } else {
            hasPrev = true;
            prevEdgetype = vertex.getEdgeList(EDGETYPE.RF).getCountOfPosition() > 0 ? EDGETYPE.RF : EDGETYPE.RR; 
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
            voteToHalt();  // this node can never merge (restricted by neighbors or my structure)
        } else {
            if (curHead) {
                if (hasNext && !nextHead) {
                    // compress this head to the forward tail
                    setMerge((byte) (nextEdgetype.get() | P4State.MERGE));
                } else if (hasPrev && !prevHead) {
                    // compress this head to the reverse tail
                    setMerge((byte) (prevEdgetype.get() | P4State.MERGE));
                } 
            }
            else {
                // I'm a tail
                if (hasNext && hasPrev) {
                     if ((!nextHead && !prevHead) && (curKmer.compareTo(nextKmer) < 0 && curKmer.compareTo(prevKmer) < 0)) {
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
//        if ((getVertexValue().getState() & P4State.MERGE) == 0) {
//            LOG.info("No merge for " + getVertexId());
//        } else {
//            LOG.info("Merge from " + getVertexId() + " towards " + (getVertexValue().getState() & DirectionFlag.DIR_MASK) + "; node is " + getVertexValue());
//        }
    }
    
    public void updateNeighbors() {
        VertexValueWritable vertex = getVertexValue();
        short state = vertex.getState();
        EDGETYPE edgeType = EDGETYPE.fromByte(state);
        if ((state & P4State.MERGE) == 0) {
            return;  // no merge requested; don't have to update neighbors
        }
        
        DIR mergeDir = edgeType.dir(); 
        EnumSet<EDGETYPE> mergeEdges = NodeWritable.edgeTypesInDir(mergeDir);
        
        DIR updateDir = mergeDir.mirror();
        EnumSet<EDGETYPE> updateEdges = NodeWritable.edgeTypesInDir(updateDir);  
        
        // prepare the update message s.t. the receiver can do a simple unionupdate
        // that means we figure out any hops and place our merge-dir edges in the appropriate list of the outgoing msg
        for (EDGETYPE updateEdge : updateEdges) {
            outgoingMsg.reset();
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setFlag(updateEdge.mirror().get());  // neighbor's edge to me (so he can remove me) 
            for (EDGETYPE mergeEdge : mergeEdges) {
                EDGETYPE newEdgetype = EDGETYPE.resolveLinkThroughMiddleNode(updateEdge, mergeEdge);
                outgoingMsg.getNode().setEdgeList(newEdgetype, getVertexValue().getEdgeList(mergeEdge));  // copy into outgoingMsg
            }
            
            // send the update to all kmers in this list // TODO perhaps we could skip all this if there are no neighbors here
            for (VKmerBytesWritable dest : vertex.getEdgeList(updateEdge).getKeys()) {
//                LOG.info("send update message from " + getVertexId() + " to " + dest + ": " + outgoingMsg);
                sendMsg(dest, outgoingMsg);
            }
        }
    }
    
    public void receiveUpdates(Iterator<PathMergeMessageWritable> msgIterator){
        VertexValueWritable vertex = getVertexValue(); 
        NodeWritable node = vertex.getNode();
        boolean updated = false;
        while (msgIterator.hasNext()) {
//            LOG.info("before update from neighbor: " + getVertexValue());
            incomingMsg = msgIterator.next();
            // remove the edge to the node that will merge elsewhere
            node.getEdgeList(EDGETYPE.fromByte(incomingMsg.getFlag())).remove(incomingMsg.getSourceVertexId());
            // add the node this neighbor will merge into
            for (EDGETYPE edgeType : EnumSet.allOf(EDGETYPE.class)) {
                node.getEdgeList(edgeType).unionUpdate(incomingMsg.getEdgeList(edgeType));
            }
            updated = true;
//            LOG.info("after update from neighbor: " + getVertexValue());
        }
        if (updated) {
            if (DIR.enumSetFromByte(vertex.getState()).containsAll(EnumSet.allOf(DIR.class)))
                voteToHalt();
            else 
                activate();
        }
    }
    
    public void sendMergeMsg() {
        VertexValueWritable vertex = getVertexValue();
        short state = vertex.getState();
        if ((state & P4State.MERGE) != 0) {
            outgoingMsg.reset();
            // tell neighbor where this is coming from (so they can merge kmers and delete)
            EDGETYPE mergeEdgetype = EDGETYPE.fromByte(vertex.getState());
            byte neighborRestrictions = DIR.fromSet(mergeEdgetype.causesFlip() ? DIR.flipSetFromByte(state) : DIR.enumSetFromByte(state));
            
            outgoingMsg.setFlag((short) (mergeEdgetype.mirror().get() | neighborRestrictions));
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setNode(vertex.getNode());
            if (vertex.getDegree(mergeEdgetype.dir()) != 1)
                throw new IllegalStateException("Merge attempted in node with degree in " + mergeEdgetype + " direction != 1!\n" + vertex);
            VKmerBytesWritable dest = vertex.getEdgeList(mergeEdgetype).get(0).getKey();
//            LOG.info("send merge mesage from " + getVertexId() + " to " + dest + ": " + outgoingMsg + "; my restrictions are: " + DIR.enumSetFromByte(vertex.getState()) + ", their restrictions are: " + DIR.enumSetFromByte(outgoingMsg.getFlag()));
            sendMsg(dest, outgoingMsg);
            
//            LOG.info("killing self: " + getVertexId());
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
        EDGETYPE senderEdgetype;
        @SuppressWarnings("unused")
        int numMerged = 0;
        while (msgIterator.hasNext()) {
//            LOG.info("before merge: " + getVertexValue() + " restrictions: " + DIR.enumSetFromByte(state));
            incomingMsg = msgIterator.next();
            senderEdgetype = EDGETYPE.fromByte(incomingMsg.getFlag());
            node.mergeWithNode(senderEdgetype, incomingMsg.getNode());
            state |= (byte) (incomingMsg.getFlag() & DIR.MASK);  // update incoming restricted directions
            numMerged++;
            updated = true;
//            LOG.info("after merge: " + getVertexValue() + " restrictions: " + DIR.enumSetFromByte(state));
        }
        if(isTandemRepeat(getVertexValue())) {
            // tandem repeats can't merge anymore; restrict all future merges
            state |= DIR.NEXT.get();
            state |= DIR.PREVIOUS.get();
            updated = true;
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
    public void compute(Iterator<PathMergeMessageWritable> msgIterator) {
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
            sendMergeMsg();
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, P4ForPathMergeVertex.class));
    }
}
