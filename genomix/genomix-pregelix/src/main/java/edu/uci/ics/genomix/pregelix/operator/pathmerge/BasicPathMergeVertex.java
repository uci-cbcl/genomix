package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable.State;

/**
 * The super class of different path merge algorithms
 * This maximally compresses linear subgraphs (A->B->C->...->Z) into into individual nodes (ABC...Z).
 */
public abstract class BasicPathMergeVertex<V extends VertexValueWritable, M extends PathMergeMessage> extends
        DeBruijnGraphCleanVertex<V, M> {

    private static final Logger LOG = Logger.getLogger(BasicPathMergeVertex.class.getName());

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
                if (vertex.degree(dir) > 1 || vertex.degree(dir) == 0) {
                    dirsToRestrict.add(dir);
                    state |= dir.get();
                    updated = true;
                }
            }
        }
        if (updated) {
            vertex.setState(state);
            if (DIR.enumSetFromByte(state).containsAll(Arrays.asList(DIR.values())))
                voteToHalt();
            else
                activate();
        }

        // send a message to each neighbor indicating they can't merge towards me
        for (DIR dir : dirsToRestrict) {
            for (EDGETYPE et : dir.edgeTypes()) {
                for (VKmer destId : vertex.getEdges(et)) {
                    outgoingMsg.reset();
                    outgoingMsg.setFlag(et.mirror().dir().get());
                    if (verbose)
                        LOG.fine("Iteration " + getSuperstep() + "\r\n" + "send restriction from " + getVertexId()
                                + " to " + destId + " in my " + et + " and their " + et.mirror() + " ("
                                + EDGETYPE.dir(et.mirror()) + "); I am " + getVertexValue());
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
                LOG.fine("Iteration " + getSuperstep() + "\r\n" + "before restriction " + getVertexId() + ": "
                        + DIR.enumSetFromByte(restrictedDirs));
            M incomingMsg = msgIterator.next();
            restrictedDirs |= incomingMsg.getFlag();
            if (verbose)
                LOG.fine("after restriction " + getVertexId() + ": " + DIR.enumSetFromByte(restrictedDirs));
            updated = true;
        }
        if (updated) {
            getVertexValue().setState(restrictedDirs);
            if (DIR.enumSetFromByte(restrictedDirs).containsAll(Arrays.asList(DIR.values())))
                voteToHalt();
            else
                activate();
        }
    }

    public void updateNeighbors() {
        VertexValueWritable vertex = getVertexValue();
        short state = vertex.getState();
        EDGETYPE edgeType = EDGETYPE.fromByte(state);
        if ((state & State.MERGE) == 0) {
            return; // no merge requested; don't have to update neighbors
        }

        DIR mergeDir = edgeType.dir();
        EDGETYPE[] mergeEdges = mergeDir.edgeTypes();

        DIR updateDir = mergeDir.mirror();
        EDGETYPE[] updateEdges = updateDir.edgeTypes();

        // prepare the update message s.t. the receiver can do a simple unionupdate
        // that means we figure out any hops and place our merge-dir edges in the appropriate list of the outgoing msg
        for (EDGETYPE updateEdge : updateEdges) {
            outFlag = (byte) (MESSAGETYPE.UPDATE.get() | updateEdge.mirror().get()); // neighbor's edge to me (so he can remove me)
            for (VKmer dest : vertex.getEdges(updateEdge)) {
                for (EDGETYPE mergeEdge : mergeEdges) {
                    for (VKmer kmer : vertex.getEdges(mergeEdge)) {
                        outgoingMsg.reset();
                        outgoingMsg.setSourceVertexId(getVertexId());
                        outgoingMsg.setFlag(outFlag);
                        outgoingMsg.getNode().getEdges(EDGETYPE.resolveEdgeThroughPath(updateEdge, mergeEdge)).append(kmer);
                        if (verbose)
                            LOG.fine("Iteration " + getSuperstep() + "\r\n" + "send update message from " + getVertexId()
                                    + " to " + dest + ": " + outgoingMsg);
                        sendMsg(dest, outgoingMsg);
                    }
                }
            }
        }
    }

    public void receiveUpdates(Iterator<M> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        boolean updated = false;
        ArrayList<PathMergeMessage> allSeenMsgs = new ArrayList<PathMergeMessage>();
        while (msgIterator.hasNext()) {
            M incomingMsg = msgIterator.next();
            if (verbose)
                LOG.fine("Iteration " + getSuperstep() + "\r\n" + "before update from neighbor: " + getVertexValue());
            // remove the edge to the node that will merge elsewhere
            if (incomingMsg.getSourceVertexId().toString().equals("AGCTAAATG")) {
                System.out.println();
            }
            vertex.getEdges(EDGETYPE.fromByte(incomingMsg.getFlag())).remove(incomingMsg.getSourceVertexId());
            // add the node this neighbor will merge into
            for (EDGETYPE edgeType : EDGETYPE.values) {
                vertex.getEdges(edgeType).unionUpdate(incomingMsg.getEdges(edgeType));
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
        if ((state & State.MERGE) != 0) {
            outgoingMsg.reset();
            // tell neighbor where this is coming from (so they can merge kmers and delete)
            EDGETYPE mergeEdgetype = EDGETYPE.fromByte(vertex.getState());
            byte neighborRestrictions = DIR.fromSet(mergeEdgetype.causesFlip() ? DIR.flipSetFromByte(state) : DIR
                    .enumSetFromByte(state));

            outgoingMsg.setFlag((short) (mergeEdgetype.mirror().get() | neighborRestrictions));
            Node outNode = outgoingMsg.getNode();
            // set only relevant edges
            for (EDGETYPE et : mergeEdgetype.mirror().neighborDir().edgeTypes()) {
                outNode.setEdges(et, vertex.getEdges(et));
            }
            outNode.setUnflippedReadIds(vertex.getUnflippedReadIds());
            outNode.setFlippedReadIds(vertex.getFlippedReadIds());
            outNode.setAverageCoverage(vertex.getAverageCoverage());
            // only send non-overlapping letters // TODO do something more efficient than toString?
            if (mergeEdgetype.mirror().neighborDir() == DIR.FORWARD) {
                outNode.getInternalKmer().setAsCopy(
                        vertex.getInternalKmer().toString().substring(Kmer.getKmerLength() - 1));
            } else {
                outNode.getInternalKmer()
                        .setAsCopy(
                                vertex.getInternalKmer()
                                        .toString()
                                        .substring(
                                                0,
                                                vertex.getInternalKmer().getKmerLetterLength() - Kmer.getKmerLength()
                                                        + 1));
            }

            if (vertex.degree(mergeEdgetype.dir()) != 1)
                throw new IllegalStateException("Merge attempted in node with degree in " + mergeEdgetype
                        + " direction != 1!\n" + vertex);
            VKmer dest = vertex.getEdges(mergeEdgetype).getPosition(0);
            sendMsg(dest, outgoingMsg);

            if (verbose) {
                LOG.fine("Iteration " + getSuperstep() + "\r\n" + "send merge mesage from " + getVertexId() + " to "
                        + dest + ": " + outgoingMsg + "; my restrictions are: "
                        + DIR.enumSetFromByte(vertex.getState()) + ", their restrictions are: "
                        + DIR.enumSetFromByte(outgoingMsg.getFlag()));
            }
        }
    }

}
