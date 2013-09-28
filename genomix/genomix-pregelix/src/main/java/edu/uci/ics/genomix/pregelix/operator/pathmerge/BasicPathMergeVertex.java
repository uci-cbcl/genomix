package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.P4State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.log.LogUtil;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag.MESSAGETYPE;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

/**
 * The super class of different path merge algorithms
 * This maximally compresses linear subgraphs (A->B->C->...->Z) into into individual nodes (ABC...Z). 
 * @author anbangx
 *
 */
public abstract class BasicPathMergeVertex<V extends VertexValueWritable, M extends PathMergeMessageWritable> extends
        BasicGraphCleanVertex<V, M> {

    private static final Logger LOG = Logger.getLogger(BasicPathMergeVertex.class.getName());

    /**
     * Logging the vertexId and vertexValue
     */
    public void loggingNode(byte loggingType) {
        String logMessage = LogUtil.getVertexLog(loggingType, getSuperstep(), getVertexId(), getVertexValue());
        LOG.fine(logMessage);
    }

    /**
     * Logging message
     */
    public void loggingMessage(byte loggingType, PathMergeMessageWritable msg, VKmerBytesWritable dest) {
        String logMessage = LogUtil.getMessageLog(loggingType, getSuperstep(), getVertexId(), msg, dest);
        LOG.fine(logMessage);
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
            dirsToRestrict = EnumSet.of(DIR.FORWARD, DIR.REVERSE);
            state |= DIR.FORWARD.get();
            state |= DIR.REVERSE.get();
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
                        LOG.fine("Iteration " + getSuperstep() + "\r\n" 
                                + "send restriction from " + getVertexId() + " to " + destId + " in my " + et
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
        short restrictedDirs = getVertexValue().getState(); // the directions (FORWARD/REVERSE) that I'm not allowed to merge in
        boolean updated = false;
        while (msgIterator.hasNext()) {
            if (verbose)
                LOG.fine("Iteration " + getSuperstep() + "\r\n" 
                        + "before restriction " + getVertexId() + ": " + DIR.enumSetFromByte(restrictedDirs));
            M incomingMsg = msgIterator.next();
            restrictedDirs |= incomingMsg.getFlag();
            if (verbose)
                LOG.fine("after restriction " + getVertexId() + ": " + DIR.enumSetFromByte(restrictedDirs));
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
            outFlag |= MESSAGETYPE.UPDATE.get() | updateEdge.mirror().get(); // neighbor's edge to me (so he can remove me)
            outgoingMsg.setFlag(outFlag);
            for (EDGETYPE mergeEdge : mergeEdges) {
                EDGETYPE newEdgetype = EDGETYPE.resolveEdgeThroughPath(updateEdge, mergeEdge);
                outgoingMsg.getNode().setEdgeList(newEdgetype, getVertexValue().getEdgeList(mergeEdge)); // copy into outgoingMsg
            }

            // send the update to all kmers in this list // TODO perhaps we could skip all this if there are no neighbors here
            for (VKmerBytesWritable dest : vertex.getEdgeList(updateEdge).getKeys()) {
                if (verbose)
                    LOG.fine("Iteration " + getSuperstep() + "\r\n" 
                            + "send update message from " + getVertexId() + " to " + dest + ": " + outgoingMsg);
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
            M incomingMsg = msgIterator.next();
            if (verbose)
                LOG.fine("Iteration " + getSuperstep() + "\r\n" 
                        + "before update from neighbor: " + getVertexValue());
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

            if (verbose) {
                LOG.fine("Iteration " + getSuperstep() + "\r\n" 
                        + "send merge mesage from " + getVertexId() + " to " + dest + ": " + outgoingMsg
                        + "; my restrictions are: " + DIR.enumSetFromByte(vertex.getState())
                        + ", their restrictions are: " + DIR.enumSetFromByte(outgoingMsg.getFlag()));
            }
        }
    }

}
