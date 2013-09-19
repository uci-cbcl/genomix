package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

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

    protected boolean isNodeRandomHead(VKmerBytesWritable nodeKmer) {
        // "deterministically random", based on node id
        randGenerator.setSeed((randSeed ^ nodeKmer.hashCode()) * 10000 * getSuperstep());//randSeed + nodeID.hashCode()
        for(int i = 0; i < 500; i++)
            randGenerator.nextFloat();
        boolean isHead = randGenerator.nextFloat() < probBeingRandomHead;
        LOG.info("randomHead: " + nodeKmer + "=" + isHead);
        return isHead;
    }
    
    /**
     * checks if there is a valid, mergeable neighbor in the given direction.  sets hasNext/Prev, next/prevDir, Kmer and Head
     */
    protected void checkNeighbors() {
        VertexValueWritable vertex = getVertexValue();
        EnumSet<DIR> restrictedDirs = DIR.enumSetFromByte(vertex.getState());
        // NEXT restricted by neighbor or by my edges? 
        if (restrictedDirs.contains(DIR.NEXT) || vertex.outDegree() != 1) {
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
        
        if (!hasNext && !hasPrev) {
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
        LOG.info("merge dir chosen:" + getVertexValue());
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
        byte[] updateEdges = NodeWritable.edgeTypesInDir(updateDir);
        
        // prepare the update message s.t. the receiver can do a simple unionupdate
        // that means we figure out any hops and place our merge-dir edges in the appropriate list of the outgoing msg
        for (byte updateEdge : updateEdges) {
            outgoingMsg.reset();
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setFlag(updateEdge);  // neighbor's edge to me (so he can remove me) 
            for (byte mergeEdge : mergeEdges) {
                byte newDir = DirectionFlag.resolveLinkThroughMiddleNode(updateEdge, mergeEdge);
                outgoingMsg.getNode().setEdgeList(newDir, getVertexValue().getEdgeList(mergeEdge));  // copy into outgoingMsg
            }
            
            // send the update to all kmers in this list // TODO perhaps we could skip all this if there are no neighbors here
            for (VKmerBytesWritable dest : vertex.getEdgeList(updateEdge).getKeys()) {
                sendMsg(dest, outgoingMsg);
            }
        }
    }
    
    public void receiveUpdates(Iterator<PathMergeMessageWritable> msgIterator){
        VertexValueWritable vertex = getVertexValue(); 
        NodeWritable node = vertex.getNode();
        LOG.info("before update from neighbor: " + getVertexValue());
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            // remove the edge to the node that will merge elsewhere
            node.getEdgeList((byte)(incomingMsg.getFlag() & DirectionFlag.DIR_MASK)).remove(incomingMsg.getSourceVertexId());
            // add the node this neighbor will merge into
            for (byte dir : DirectionFlag.values) {
                node.getEdgeList(dir).unionUpdate(incomingMsg.getEdgeList(dir));
            }
        }
        checkNeighbors();
        if (!hasNext && !hasPrev)
            voteToHalt();
        else
            activate();
        LOG.info("after update from neighbor: " + getVertexValue());
    }
    
    public void broadcastMerge() {
        VertexValueWritable vertex = getVertexValue();
        if ((vertex.getState() & P4State.MERGE) != 0) {
            outgoingMsg.reset();
            // tell neighbor where this is coming from (so they can merge kmers and delete)
            byte mergeDir = (byte)(vertex.getState() & DirectionFlag.DIR_MASK);
            byte restrictedDirs = (byte)(vertex.getState() & DIR.MASK);
            outgoingMsg.setFlag((short) (DirectionFlag.mirrorEdge(mergeDir) | restrictedDirs)); // TODO make the node flipped...
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setNode(vertex.getNode());
            if (vertex.getDegree(DirectionFlag.dirFromEdgeType(mergeDir)) != 1)
                throw new IllegalStateException("Merge attempted in node with degree in " + mergeDir + " direction != 1!\n" + vertex);
            VKmerBytesWritable dest = vertex.getEdgeList(mergeDir).get(0).getKey();
            sendMsg(dest, outgoingMsg);
        }
    }
    
    /**
     * step4: processMerges 
     */
    public void receiveMerges(Iterator<PathMergeMessageWritable> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        NodeWritable node = vertex.getNode();
        LOG.info("before merge: " + getVertexValue());
        short state = vertex.getState(); 
        byte senderDir;
        int numMerged = 0;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            senderDir = (byte) (incomingMsg.getFlag() & DirectionFlag.DIR_MASK);
            node.mergeWithNode(senderDir, node);
            state |= (byte) (incomingMsg.getFlag() & DIR.MASK);  // update incoming restricted directions
            numMerged++;
        }
        if(isTandemRepeat(getVertexValue())) {
            state |= (DIR.NEXT.get() | DIR.PREVIOUS.get());  // tandem repeats can't merge anymore
//          updateStatisticsCounter(StatisticsCounter.Num_Cycles); 
        }
//      updateStatisticsCounter(StatisticsCounter.Num_MergedNodes);
//      getVertexValue().setCounters(counters);
        vertex.setState(state);
        
        LOG.info("after merge: " + getVertexValue());
    }
    
    @Override
    public void compute(Iterator<PathMergeMessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() > 4) {
            LOG.info("test");
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
            broadcastMerge();
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, P4ForPathMergeVertex.class));
    }
}
