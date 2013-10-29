package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessage;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.MessageFlag.MESSAGETYPE;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;

public class P1ForPathMergeVertex extends BasicPathMergeVertex<VertexValueWritable, PathMergeMessage> {

    private static final Logger LOG = Logger.getLogger(P1ForPathMergeVertex.class.getName());

    private HashSet<PathMergeMessage> updateMsgs = new HashSet<PathMergeMessage>();
    private HashSet<PathMergeMessage> neighborMsgs = new HashSet<PathMergeMessage>();

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
        if (repeatKmer == null)
            repeatKmer = new VKmer();
        if (getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
        //        else
        //            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }

    public void chooseMergeDir() {
        VertexValueWritable vertex = getVertexValue();
        short state = vertex.getState();
        EnumSet<DIR> restrictedDirs = DIR.enumSetFromByte(state);
        boolean updated = false;
        //initiate merge dir
        state &= State.MERGE_CLEAR;
        state |= State.NO_MERGE; //setMerge(P4State.NO_MERGE);

        //choose merge dir -- principle: only merge with nextDir
        if (restrictedDirs.size() == 1) {
            if ((restrictedDirs.contains(DIR.REVERSE) && vertex.degree(DIR.FORWARD) == 1)
                    || (restrictedDirs.contains(DIR.FORWARD) && vertex.degree(DIR.REVERSE) == 1)) {
                EDGETYPE edgeType = restrictedDirs.contains(DIR.REVERSE) ? vertex.getNeighborEdgeType(DIR.FORWARD)
                        : vertex.getNeighborEdgeType(DIR.REVERSE);
                state |= State.MERGE | edgeType.get();
                updated = true;
            }
        }

        getVertexValue().setState(state);
        if (updated)
            activate();
        else
            voteToHalt();

        if (verbose) {
            LOG.fine("Iteration " + getSuperstep() + "\r\n" + "Mark: Merge from " + getVertexId() + " towards "
                    + (EDGETYPE.fromByte(getVertexValue().getState())) + "; node is " + getVertexValue());
        }
    }

    /**
     * step4: receive and process Merges for P1
     */
    public void receiveMerges(Iterator<PathMergeMessage> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        Node node = vertex.getNode();
        short state = vertex.getState();
        boolean updated = false;
        EDGETYPE senderEdgetype;
        @SuppressWarnings("unused")
        int numMerged = 0;

        // aggregate incomingMsg
        ArrayList<PathMergeMessage> receivedMsgList = new ArrayList<PathMergeMessage>();
        while (msgIterator.hasNext())
            receivedMsgList.add(new PathMergeMessage(msgIterator.next()));

        if (receivedMsgList.size() > 2)
            throw new IllegalStateException("In path merge, it is impossible to receive more than 2 messages!");

        // odd number of nodes
        if (receivedMsgList.size() == 2) {
            if (verbose)
                LOG.fine("Iteration " + getSuperstep() + "\r\n" + "before merge: " + getVertexValue()
                        + " restrictions: " + DIR.enumSetFromByte(state));
            for (PathMergeMessage msg : receivedMsgList) {
                senderEdgetype = EDGETYPE.fromByte(msg.getFlag());
                node.mergeWithNode(senderEdgetype, msg.getNode());
                state |= (byte) (msg.getFlag() & DIR.MASK); // update incoming restricted directions
                numMerged++;
                updated = true;
                deleteVertex(msg.getSourceVertexId());
                LOG.fine("killing self: " + msg.getSourceVertexId());
            }
            if (verbose)
                LOG.fine("after merge: " + getVertexValue() + " restrictions: " + DIR.enumSetFromByte(state));
        } else if (receivedMsgList.size() == 1) { // even number of nodes
            if (verbose)
                LOG.fine("Iteration " + getSuperstep() + "\r\n" + "before merge: " + getVertexValue()
                        + " restrictions: " + DIR.enumSetFromByte(state));
            PathMergeMessage msg = receivedMsgList.get(0);
            senderEdgetype = EDGETYPE.fromByte(msg.getFlag());
            state |= (byte) (msg.getFlag() & DIR.MASK); // update incoming restricted directions
            VKmer me = getVertexId();
            VKmer other = msg.getSourceVertexId();
            // determine if merge. if head msg meets head and #receiveMsg = 1
            if (DIR.enumSetFromByte(state).containsAll(EnumSet.allOf(DIR.class))) {
                if (me.compareTo(other) < 0) {
                    node.mergeWithNode(senderEdgetype, msg.getNode());
                    numMerged++;
                    updated = true;
                    deleteVertex(other);
                } else {
                    // broadcast kill self and update pointer to new kmer, update edge from toMe to toOther
                    node.mergeWithNode(senderEdgetype, msg.getNode());
                    outgoingMsg.setSourceVertexId(me);
                    outgoingMsg.getNode().setInternalKmer(other);
                    outFlag = 0;
                    outFlag |= MESSAGETYPE.TO_NEIGHBOR.get();
                    for (EDGETYPE et : EnumSet.allOf(EDGETYPE.class)) {
                        for (VKmer dest : vertex.getEdgeMap(et).keySet()) {
                            EDGETYPE meToNeighbor = et.mirror();
                            EDGETYPE otherToNeighbor = senderEdgetype.causesFlip() ? meToNeighbor.flipNeighbor()
                                    : meToNeighbor;
                            outFlag &= EDGETYPE.CLEAR;
                            outFlag &= MessageFlag.MERGE_DIR_CLEAR;
                            outFlag |= meToNeighbor.get() | otherToNeighbor.get() << 9;
                            outgoingMsg.setFlag(outFlag);
                            sendMsg(dest, outgoingMsg);
                        }
                    }

                    state |= State.NO_MERGE;
                    vertex.setState(state);
                    voteToHalt();
                }
            } else {
                node.mergeWithNode(senderEdgetype, msg.getNode());
                numMerged++;
                updated = true;
                deleteVertex(other);
                LOG.fine("killing self: " + other);
            }
            if (verbose)
                LOG.fine("after merge: " + getVertexValue() + " restrictions: " + DIR.enumSetFromByte(state));
        }

        if (isTandemRepeat(getVertexValue())) {
            // tandem repeats can't merge anymore; restrict all future merges
            state |= DIR.FORWARD.get();
            state |= DIR.REVERSE.get();
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

    public void catagorizeMsg(Iterator<PathMergeMessage> msgIterator) {
        updateMsgs.clear();
        neighborMsgs.clear();
        while (msgIterator.hasNext()) {
            PathMergeMessage incomingMsg = msgIterator.next();
            MESSAGETYPE msgType = MESSAGETYPE.fromByte(incomingMsg.getFlag());
            switch (msgType) {
                case UPDATE:
                    updateMsgs.add(new PathMergeMessage(incomingMsg));
                    break;
                case TO_NEIGHBOR:
                    neighborMsgs.add(new PathMergeMessage(incomingMsg));
                    break;
                default:
                    throw new IllegalStateException("Message types are allowd for only TO_UPDATE and TO_NEIGHBOR!");
            }
        }
    }

    public void receiveToNeighbor(Iterator<PathMergeMessage> msgIterator) {
        VertexValueWritable value = getVertexValue();
        if (verbose)
            LOG.fine("Iteration " + getSuperstep() + "\r\n" + "before update from dead vertex: " + value);
        while (msgIterator.hasNext()) {
            PathMergeMessage incomingMsg = msgIterator.next();
            EDGETYPE deleteToMe = EDGETYPE.fromByte(incomingMsg.getFlag());
            EDGETYPE aliveToMe = EDGETYPE.fromByte((short) (incomingMsg.getFlag() >> 9));

            VKmer deletedKmer = incomingMsg.getSourceVertexId();
            if (value.getEdgeMap(deleteToMe).containsKey(deletedKmer)) {
                ReadIdSet deletedReadIds = value.getEdgeMap(deleteToMe).get(deletedKmer);
                value.getEdgeMap(deleteToMe).remove(deletedKmer);

                value.getEdgeMap(aliveToMe).unionAdd(incomingMsg.getInternalKmer(), deletedReadIds);
            }
            voteToHalt();
        }
        if (verbose)
            LOG.fine("after update from dead vertex: " + value);
    }

    /**
     * for P1
     */
    @Override
    public void sendMergeMsg() {
        super.sendMergeMsg();
        short state = getVertexValue().getState();
        if ((getVertexValue().getState() & State.MERGE) != 0) {
            // set flag to NO_MERGE instead of deleteVertex
            state |= State.NO_MERGE;
            getVertexValue().setState(state);
            voteToHalt();
        }
    }

    @Override
    public void compute(Iterator<PathMergeMessage> msgIterator) throws Exception {
        initVertex();
        if (getSuperstep() > maxIteration) {
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
            catagorizeMsg(msgIterator);

            receiveUpdates(updateMsgs.iterator());
            receiveToNeighbor(neighborMsgs.iterator());

            sendMergeMsg();
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, P1ForPathMergeVertex.class));
    }
}
