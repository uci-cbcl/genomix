package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.P4State;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.log.LogUtil;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public abstract class BasicPathMergeVertex<V extends VertexValueWritable, M extends PathMergeMessageWritable> extends
        BasicGraphCleanVertex<V, M> {

    private static final Logger LOG = Logger.getLogger(BasicPathMergeVertex.class.getName());

    protected static final boolean isP1 = true;
    protected static final boolean isP2 = false;
    protected static final boolean isP4 = true;

    public byte getHeadMergeDir() {
        return (byte) (getVertexValue().getState() & State.HEAD_CAN_MERGE_MASK);
    }

    public byte getMsgMergeDir() {
        return (byte) (incomingMsg.getFlag() & MessageFlag.HEAD_CAN_MERGE_MASK);
    }

    public byte getAggregatingMsgMergeDir() {
        return (byte) (aggregatingMsg.getFlag() & MessageFlag.HEAD_CAN_MERGE_MASK);
    }

    public boolean isHeadUnableToMerge() {
        byte state = (byte) (getVertexValue().getState() & State.HEAD_CAN_MERGE_MASK);
        return state == State.HEAD_CANNOT_MERGE;
    }

    public DIR revert(DIR direction) {
        return direction == DIR.PREVIOUS ? DIR.NEXT : DIR.PREVIOUS;
    }

    /**
     * send UPDATE msg boolean: true == P4, false == P2
     */
    public void sendUpdateMsg(boolean isP4, DIR direction) {
        // TODO pass in the vertexId rather than isP4 (removes this block）
        //        if(isP4)
        //            outgoingMsg.setFlip(ifFlipWithNeighbor(revertDirection)); //ifFilpWithSuccessor()
        //        else 
        //            outgoingMsg.setFlip(ifFilpWithSuccessor(incomingMsg.getSourceVertexId()));

        DIR revertDirection = revert(direction);
        EnumSet<EDGETYPE> mergeDirs = direction == DIR.PREVIOUS ? EDGETYPE.OUTGOING : EDGETYPE.INCOMING;
        EnumSet<EDGETYPE> updateDirs = direction == DIR.PREVIOUS ? EDGETYPE.INCOMING : EDGETYPE.OUTGOING;

        //set deleteKmer
        outgoingMsg.setSourceVertexId(getVertexId());

        //set replaceDir
        setReplaceDir(mergeDirs);

        for (EDGETYPE dir : updateDirs) {
            kmerIterator = getVertexValue().getEdgeList(dir).getKeyIterator();
            while (kmerIterator.hasNext()) {
                //set deleteDir
                EDGETYPE deleteDir = setDeleteDir(dir);
                //set mergeDir, so it won't need flip
                setMergeDir(deleteDir, revertDirection);
                outgoingMsg.setFlag(outFlag);
                destVertexId = kmerIterator.next(); //TODO does destVertexId need deep copy?
                sendMsg(destVertexId, outgoingMsg);
            }
        }
    }

    /**
     * updateAdjList
     */
    public void processUpdate(M msg) {
        // A -> B -> C with B merging with C
        inFlag = msg.getFlag();
        EDGETYPE deleteDir = EDGETYPE.fromByte((short) ((inFlag & MessageFlag.DELETE_DIR_MASK) >> 11)); // B -> A dir
        EDGETYPE mergeDir = EDGETYPE.fromByte((short) ((inFlag & MessageFlag.MERGE_DIR_MASK) >> 9)); // C -> A dir
        EDGETYPE replaceDir = EDGETYPE.fromByte((short) (inFlag & MessageFlag.REPLACE_DIR_MASK)); // C -> B dir

        getVertexValue().getNode().updateEdges(deleteDir, msg.getSourceVertexId(), mergeDir, replaceDir, msg.getNode(),
                true);
    }

    /**
     * send MERGE msg
     */
    public void sendMergeMsg(boolean isP4) {
        byte restrictedDirs = (byte) (getVertexValue().getState() & DIR.MASK);
        outFlag |= restrictedDirs;

        DIR direction = null;
        byte mergeDir = (byte) (getVertexValue().getState() & State.CAN_MERGE_MASK);
        if (mergeDir == State.CAN_MERGEWITHPREV)
            direction = DIR.PREVIOUS;
        else if (mergeDir == State.CAN_MERGEWITHNEXT)
            direction = DIR.NEXT;
        if (direction != null) {
            setNeighborToMeDir(direction);
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setNode(getVertexValue().getNode()); //half of edges are enough
            destVertexId = getDestVertexId(direction);
            sendMsg(destVertexId, outgoingMsg);

            if (isP4)
                deleteVertex(getVertexId());
            else {
                getVertexValue().setState(State.IS_DEAD);
                activate();
            }
        }
    }

    public void aggregateMsg(Iterator<M> msgIterator) {
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            //aggregate all the incomingMsgs first
            switch (getAggregatingMsgMergeDir()) {
                case State.PATH_NON_HEAD:
                    aggregatingMsg.setFlag(getMsgMergeDir());
                    activate();
                    break;
                case State.HEAD_CAN_MERGEWITHPREV:
                case State.HEAD_CAN_MERGEWITHNEXT:
                    if (getAggregatingMsgMergeDir() != getMsgMergeDir())
                        aggregatingMsg.setFlag(State.HEAD_CANNOT_MERGE);
                    break;
                case State.HEAD_CANNOT_MERGE:
                    break;
            }
        }
    }

    public void updateState() {
        switch (getHeadMergeDir()) {
            case State.PATH_NON_HEAD:
                getVertexValue().setState(getAggregatingMsgMergeDir());
                activate();
                break;
            case State.HEAD_CAN_MERGEWITHPREV:
            case State.HEAD_CAN_MERGEWITHNEXT:
                if (getHeadMergeDir() != getAggregatingMsgMergeDir()) {
                    getVertexValue().setState(State.HEAD_CANNOT_MERGE);
                    voteToHalt();
                }
                break;
            case State.HEAD_CANNOT_MERGE:
                voteToHalt();
                break;
        }
    }

    public void setReplaceDir(EnumSet<EDGETYPE> mergeDirs) {
        EDGETYPE replaceDir = null;
        for (EDGETYPE dir : mergeDirs) {
            int num = getVertexValue().getEdgeList(dir).getCountOfPosition();
            if (num > 0) {
                if (num != 1)
                    throw new IllegalStateException("Only can sendUpdateMsg to degree = 1 direction!");
                outgoingMsg.getNode().setEdgeList(dir, getVertexValue().getEdgeList(dir));
                replaceDir = dir;
                break;
            }
        }
        outFlag &= MessageFlag.REPLACE_DIR_CLEAR;
        outFlag |= replaceDir.get();
    }

    public EDGETYPE setDeleteDir(EDGETYPE dir) {
        EDGETYPE deleteDir = dir.mirror();
        outFlag &= MessageFlag.DELETE_DIR_CLEAR;
        outFlag |= deleteDir.get();
        return deleteDir;
    }

    public void setMergeDir(EDGETYPE deleteDir, DIR revertDirection) {
        EDGETYPE mergeDir = ifFlipWithNeighbor(revertDirection) ? deleteDir.flip() : deleteDir;
        outFlag &= MessageFlag.MERGE_DIR_CLEAR;
        outFlag |= mergeDir.get();
    }

    /**
     * final updateAdjList
     */
    public void processFinalUpdate() {
        inFlag = incomingMsg.getFlag();
        EDGETYPE meToNeighborDir = EDGETYPE.fromByte(incomingMsg.getFlag());
        EDGETYPE neighborToMeDir = meToNeighborDir.mirror();

        EDGETYPE neighborToMergeDir = incomingMsg.isFlip() ? neighborToMeDir.flip() : neighborToMeDir;
        getVertexValue().processFinalUpdates(neighborToMeDir, neighborToMergeDir, incomingMsg.getNode());
    }

    /**
     * final updateAdjList
     */
    public void processFinalUpdate2() {
        inFlag = incomingMsg.getFlag();
        EDGETYPE meToNeighborDir = EDGETYPE.fromByte(inFlag);
        EDGETYPE neighborToMeDir = meToNeighborDir.mirror();

        EdgeWritable edge = new EdgeWritable();
        edge.setKey(incomingMsg.getSourceVertexId());
        edge.setReadIDs(incomingMsg.getNode().getEdgeList(meToNeighborDir).getReadIDs(getVertexId()));
        getVertexValue().getEdgeList(neighborToMeDir).unionAdd(edge);
    }

    public boolean isDifferentDirWithMergeKmer(byte neighborToMeDir) {
        return neighborToMeDir == MessageFlag.DIR_FR || neighborToMeDir == MessageFlag.DIR_RF;
    }

    /**
     * check if head receives message from head
     */
    public boolean isHeadMeetsHead(boolean selfFlag) {
        boolean msgFlag = (getMsgMergeDir() == MessageFlag.HEAD_CAN_MERGEWITHPREV || getMsgMergeDir() == MessageFlag.HEAD_CAN_MERGEWITHNEXT);
        return selfFlag && msgFlag;
    }

    /**
     * check if non-head receives message from head
     */
    public boolean isNonHeadReceivedFromHead() {
        boolean selfFlag = (getHeadMergeDir() == State.HEAD_CAN_MERGEWITHPREV || getHeadMergeDir() == State.HEAD_CAN_MERGEWITHNEXT);
        boolean msgFlag = (getMsgMergeDir() == MessageFlag.HEAD_CAN_MERGEWITHPREV || getMsgMergeDir() == MessageFlag.HEAD_CAN_MERGEWITHNEXT);
        return selfFlag == false && msgFlag == true;
    }

    /**
     * merge and updateAdjList having parameter
     */
    public void processMerge(PathMergeMessageWritable msg) {
        inFlag = msg.getFlag();
        EDGETYPE meToNeighborDir = EDGETYPE.fromByte(inFlag);
        EDGETYPE neighborToMeDir = meToNeighborDir.mirror();

        short state = getVertexValue().getState();
        //        state |= flipHeadMergeDir((byte)(inFlag & DIR.MASK), isDifferentDirWithMergeKmer(neighborToMeDir));
        getVertexValue().setState(state);

        getVertexValue().processMerges(neighborToMeDir, msg.getNode(), kmerSize);
    }

    /**
     * override sendUpdateMsg and use incomingMsg as parameter automatically
     */
    public void sendUpdateMsg() {
        sendUpdateMsgForP2(incomingMsg);
    }

    public void sendFinalUpdateMsg() {
        outFlag |= MessageFlag.IS_FINAL;
        sendUpdateMsgForP2(incomingMsg);
    }

    /**
     * send update message to neighber for P2
     */
    public void sendUpdateMsgForP2(MessageWritable msg) {
        outgoingMsg.reset();
        outgoingMsg.setUpdateMsg(true);
        EDGETYPE meToNeighborDir = EDGETYPE.fromByte(incomingMsg.getFlag());
        EDGETYPE neighborToMeDir = meToNeighborDir.mirror();
        switch (neighborToMeDir) {
            case FF:
            case FR:
                sendUpdateMsg(isP2, DIR.PREVIOUS);
                break;
            case RF:
            case RR:
                sendUpdateMsg(isP2, DIR.NEXT);
                break;
        }
    }

    public void headSendUpdateMsg() {
        outgoingMsg.reset();
        outgoingMsg.setUpdateMsg(true);
        switch (getVertexValue().getState() & MessageFlag.HEAD_CAN_MERGE_MASK) {
            case MessageFlag.HEAD_CAN_MERGEWITHPREV:
                sendUpdateMsg(isP2, DIR.NEXT);
                break;
            case MessageFlag.HEAD_CAN_MERGEWITHNEXT:
                sendUpdateMsg(isP2, DIR.PREVIOUS);
                break;
        }
    }

    public void sendMergeMsgToSuccessor() {
        setNeighborToMeDir(DIR.NEXT);
        if (ifFlipWithPredecessor())
            outgoingMsg.setFlip(true);
        else
            outgoingMsg.setFlip(false);
        outgoingMsg.setFlag(outFlag);
        for (EDGETYPE d : EDGETYPE.INCOMING)
            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
        sendMsg(getDestVertexId(DIR.NEXT), outgoingMsg);
    }

    public boolean canMergeWithHead(MessageWritable msg) {
        EDGETYPE meToNeighborDir = EDGETYPE.fromByte(msg.getFlag());
        EDGETYPE neighborToMeDir = meToNeighborDir.mirror();
        switch (neighborToMeDir) {
            case FF:
            case FR:
                return getVertexValue().outDegree() == 1;
            case RF:
            case RR:
                return getVertexValue().inDegree() == 1;
        }
        return false;
    }

    public void sendMergeMsgByIncomingMsgDir() {
        EDGETYPE meToNeighborDir = EDGETYPE.fromByte(incomingMsg.getFlag());
        EDGETYPE neighborToMeDir = meToNeighborDir.mirror();
        switch (neighborToMeDir) {
            case FF:
            case FR:
                configureMergeMsgForSuccessor(incomingMsg.getSourceVertexId());
                break;
            case RF:
            case RR:
                configureMergeMsgForPredecessor(incomingMsg.getSourceVertexId());
                break;
        }
    }

    /**
     * configure MERGE msg TODO: delete edgelist, merge configureMergeMsgForPredecessor and configureMergeMsgForPredecessorByIn...
     */
    public void configureMergeMsgForPredecessor(VKmerBytesWritable mergeDest) {
        setNeighborToMeDir(DIR.PREVIOUS);
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        //        for(byte d: OutgoingListFlag.values)
        //            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        outgoingMsg.setNode(getVertexValue().getNode());
        sendMsg(mergeDest, outgoingMsg);
    }

    public void configureMergeMsgForSuccessor(VKmerBytesWritable mergeDest) {
        setNeighborToMeDir(DIR.NEXT);
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        //        for(byte d: IncomingListFlag.values)
        //            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        outgoingMsg.setNode(getVertexValue().getNode());
        sendMsg(mergeDest, outgoingMsg);
    }

    public byte revertHeadMergeDir(byte headMergeDir) {
        switch (headMergeDir) {
            case MessageFlag.HEAD_CAN_MERGEWITHPREV:
                return MessageFlag.HEAD_CAN_MERGEWITHNEXT;
            case MessageFlag.HEAD_CAN_MERGEWITHNEXT:
                return MessageFlag.HEAD_CAN_MERGEWITHPREV;
        }
        return 0;

    }

    /**
     * Logging the vertexId and vertexValue
     */
    public void loggingNode(byte loggingType) {
        String logMessage = LogUtil.getVertexLog(loggingType, getSuperstep(), getVertexId(), getVertexValue());
        logger.fine(logMessage);
    }

    /**
     * Logging message
     */
    public void loggingMessage(byte loggingType, PathMergeMessageWritable msg, VKmerBytesWritable dest) {
        String logMessage = LogUtil.getMessageLog(loggingType, getSuperstep(), getVertexId(), msg, dest);
        logger.fine(logMessage);
    }

    /*
     * garbage
     */
    public void setHeadMergeDir() {
        byte state = 0;
        EDGETYPE meToNeighborDir = EDGETYPE.fromByte(incomingMsg.getFlag());
        EDGETYPE neighborToMeDir = meToNeighborDir.mirror();
        switch (neighborToMeDir) {
            case FF:
            case FR:
                state |= State.HEAD_CAN_MERGEWITHPREV;
                break;
            case RF:
            case RR:
                state |= State.HEAD_CAN_MERGEWITHNEXT;
                break;
        }
        getVertexValue().setState(state);
    }

    // 2013.9.21 --------------------------------------------------------------------------------------------------//
    /**
     * Send merge restrictions to my neighbor nodes
     */
    public void restrictNeighbors() {
        EnumSet<DIR> dirsToRestrict;
        VertexValueWritable vertex = getVertexValue();
        short state = vertex.getState();
        boolean updated = false;
        if (isTandemRepeat(vertex)) {
            // tandem repeats are not allowed to merge at all
            dirsToRestrict = EnumSet.of(DIR.NEXT, DIR.PREVIOUS);
            state |= DIR.NEXT.get();
            state |= DIR.PREVIOUS.get();
            updated = true;
        } else {
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
            for (EDGETYPE et : dir.edgeType()) {
                for (VKmerBytesWritable destId : vertex.getEdgeList(et).getKeys()) {
                    outgoingMsg.reset();
                    outgoingMsg.setFlag(et.mirror().dir().get());
                    if (verbose)
                        LOG.fine("send restriction from " + getVertexId() + " to " + destId + " in my " + et
                                + " and their " + et.mirror() + " (" + EDGETYPE.dir(et.mirror()) + "); I am "
                                + getVertexValue());
                    sendMsg(destId, outgoingMsg);
                }
            }
        }
    }

    /**
     * initiate head, rear and path node
     */
    public void recieveRestrictions(Iterator<M> msgIterator) {
        short restrictedDirs = getVertexValue().getState(); // the directions (NEXT/PREVIOUS) that I'm not allowed to merge in
        boolean updated = false;
        while (msgIterator.hasNext()) {
            if (verbose)
                LOG.fine("before restriction " + getVertexId() + ": " + DIR.fromByte(restrictedDirs));
            incomingMsg = msgIterator.next();
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

    public void updateNeighbors() {
        VertexValueWritable vertex = getVertexValue();
        short state = vertex.getState();
        EDGETYPE edgeType = EDGETYPE.fromByte(state);
        if ((state & P4State.MERGE) == 0) {
            return; // no merge requested; don't have to update neighbors
        }

        DIR mergeDir = edgeType.dir();
        EnumSet<EDGETYPE> mergeEdges = mergeDir.edgeType();

        DIR updateDir = mergeDir.mirror();
        EnumSet<EDGETYPE> updateEdges = updateDir.edgeType();

        // prepare the update message s.t. the receiver can do a simple unionupdate
        // that means we figure out any hops and place our merge-dir edges in the appropriate list of the outgoing msg
        for (EDGETYPE updateEdge : updateEdges) {
            outgoingMsg.reset();
            outgoingMsg.setSourceVertexId(getVertexId());
            outFlag = 0;
            outFlag |= MessageFlag.TO_UPDATE | updateEdge.mirror().get(); // neighbor's edge to me (so he can remove me)
            outgoingMsg.setFlag(outFlag);
            for (EDGETYPE mergeEdge : mergeEdges) {
                EDGETYPE newEdgetype = EDGETYPE.resolveLinkThroughMiddleNode(updateEdge, mergeEdge);
                outgoingMsg.getNode().setEdgeList(newEdgetype, getVertexValue().getEdgeList(mergeEdge)); // copy into outgoingMsg
            }

            // send the update to all kmers in this list // TODO perhaps we could skip all this if there are no neighbors here
            for (VKmerBytesWritable dest : vertex.getEdgeList(updateEdge).getKeys()) {
                if (verbose)
                    LOG.fine("send update message from " + getVertexId() + " to " + dest + ": " + outgoingMsg);
                sendMsg(dest, outgoingMsg);
            }
        }
    }

    public void receiveUpdates(Iterator<M> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        NodeWritable node = vertex.getNode();
        boolean updated = false;
        ArrayList<PathMergeMessageWritable> allSeenMsgs = new ArrayList<PathMergeMessageWritable>();
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if (verbose)
                LOG.fine("before update from neighbor: " + getVertexValue());
            // remove the edge to the node that will merge elsewhere
            node.getEdgeList(EDGETYPE.fromByte(incomingMsg.getFlag())).remove(incomingMsg.getSourceVertexId());
            // add the node this neighbor will merge into
            for (EDGETYPE edgeType : EnumSet.allOf(EDGETYPE.class)) {
                node.getEdgeList(edgeType).unionUpdate(incomingMsg.getEdgeList(edgeType));
            }
            updated = true;
            if (verbose) {
                LOG.fine("after update from neighbor: " + getVertexValue());
                allSeenMsgs.add(incomingMsg);
            }
        }
        if (verbose)
            LOG.fine("All recieved updates:  \n{\n" + StringUtils.join(allSeenMsgs, "\n") + "\n}\n");
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
            byte neighborRestrictions = DIR.fromSet(mergeEdgetype.causesFlip() ? DIR.flipSetFromByte(state) : DIR
                    .enumSetFromByte(state));

            outgoingMsg.setFlag((short) (mergeEdgetype.mirror().get() | neighborRestrictions));
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setNode(vertex.getNode());
            if (vertex.getDegree(mergeEdgetype.dir()) != 1)
                throw new IllegalStateException("Merge attempted in node with degree in " + mergeEdgetype
                        + " direction != 1!\n" + vertex);
            VKmerBytesWritable dest = vertex.getEdgeList(mergeEdgetype).get(0).getKey();
            sendMsg(dest, outgoingMsg);
            deleteVertex(getVertexId());

            if (verbose) {
                LOG.fine("send merge mesage from " + getVertexId() + " to " + dest + ": " + outgoingMsg
                        + "; my restrictions are: " + DIR.enumSetFromByte(vertex.getState())
                        + ", their restrictions are: " + DIR.enumSetFromByte(outgoingMsg.getFlag()));
                LOG.fine("killing self: " + getVertexId());
            }
        }
    }

    /**
     * step4: receive and process Merges
     */
    public void receiveMerges(Iterator<M> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        NodeWritable node = vertex.getNode();
        short state = vertex.getState();
        boolean updated = false;
        EDGETYPE senderEdgetype;
        @SuppressWarnings("unused")
        int numMerged = 0;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if (verbose)
                LOG.fine("before merge: " + getVertexValue() + " restrictions: " + DIR.enumSetFromByte(state));
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

}
