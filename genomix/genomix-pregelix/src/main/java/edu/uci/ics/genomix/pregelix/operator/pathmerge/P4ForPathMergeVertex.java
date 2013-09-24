package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.jfree.util.Log;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.P4State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
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
            nextEdgetype = vertex.getEdgetypeFromDir(DIR.NEXT); //getEdgeList(EDGETYPE.FF).getCountOfPosition() > 0 ? EDGETYPE.FF : EDGETYPE.FR; 
            nextKmer = vertex.getEdgeList(nextEdgetype).get(0).getKey();
            nextHead = isNodeRandomHead(nextKmer);
        }

        // PREVIOUS restricted by neighbor or by my edges? 
        if (restrictedDirs.contains(DIR.PREVIOUS) || vertex.inDegree() != 1) {
            hasPrev = false;
        } else {
            hasPrev = true;
            prevEdgetype = vertex.getEdgetypeFromDir(DIR.PREVIOUS); //vertex.getEdgeList(EDGETYPE.RF).getCountOfPosition() > 0 ? EDGETYPE.RF : EDGETYPE.RR; 
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
        if (verbose) {
//            if ((getVertexValue().getState() & P4State.MERGE) == 0) {
//                LOG.fine("No merge for " + getVertexId());
//            } else {
                LOG.fine("Merge from " + getVertexId() + " towards " + (EDGETYPE.fromByte(getVertexValue().getState())) + "; node is " + getVertexValue());
//            }
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
            sendMergeMsg();
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, P4ForPathMergeVertex.class));
    }
}
