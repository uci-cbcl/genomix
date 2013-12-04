package edu.uci.ics.genomix.pregelix.operator.checker;

import java.io.IOException;
import java.util.Iterator;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.format.SymmetryCheckerOutputFormat;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.message.SymmetryCheckerMessage;
import edu.uci.ics.genomix.pregelix.io.vertex.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.vertex.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class SymmetryCheckerVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, SymmetryCheckerMessage> {

    @Override
    public void initVertex() {
        super.initVertex();
        if (outgoingMsg == null)
            outgoingMsg = new SymmetryCheckerMessage();
        else
            outgoingMsg.reset();
        outFlag = 0;
    }

    public void sendEdges(DIR direction) {
        VertexValueWritable vertex = getVertexValue();
        for (EDGETYPE et : direction.edgeTypes()) {
            for (VKmer dest : vertex.getEdges(et)) {
                outgoingMsg.reset();
                outFlag &= EDGETYPE.CLEAR;
                outFlag |= et.mirror().get();
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setEdges(vertex.getEdges(et));
                sendMsg(dest, outgoingMsg);
            }
        }
    }

    public void sendEdgesToAllNeighborNodes() {
        sendEdges(DIR.REVERSE);
        sendEdges(DIR.FORWARD);
    }

    /**
     * check symmetry: A -> B, A'edges should have B and B's corresponding edges should have A
     * otherwise, output error vertices
     */
    public void checkSymmetry(Iterator<SymmetryCheckerMessage> msgIterator) {
        while (msgIterator.hasNext()) {
            SymmetryCheckerMessage incomingMsg = msgIterator.next();
            EDGETYPE neighborToMe = EDGETYPE.fromByte(incomingMsg.getFlag());
            boolean exist = getVertexValue().getEdges(neighborToMe).contains(incomingMsg.getSourceVertexId());
            if (!exist) {
                getVertexValue().setState(State.ERROR_NODE);
                return;
            }
            
            boolean edgesAreSame = true;
            for (VKmer kmer : incomingMsg.getEdges()) {
                if (!getVertexValue().getEdges(neighborToMe).contains(kmer)) {
                    edgesAreSame = false;
                    break;
                }
            }
            for (VKmer kmer : getVertexValue().getEdges(neighborToMe)) {
                if (!incomingMsg.getEdges().contains(kmer)) {
                    edgesAreSame = false;
                    break;
                }
            }
            if (!edgesAreSame)
                getVertexValue().setState(State.ERROR_NODE);
        }
    }

    @Override
    public void compute(Iterator<SymmetryCheckerMessage> msgIterator) throws Exception {
        initVertex();
        if (getSuperstep() == 1) {
            sendEdgesToAllNeighborNodes();
        } else if (getSuperstep() == 2) {
            //check if the corresponding edge and edges exist
            checkSymmetry(msgIterator);
        }
        voteToHalt();
    }

    public static PregelixJob getConfiguredJob(
            GenomixJobConf conf,
            Class<? extends DeBruijnGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass)
            throws IOException {
        PregelixJob job = DeBruijnGraphCleanVertex.getConfiguredJob(conf, vertexClass);
        job.setVertexOutputFormatClass(SymmetryCheckerOutputFormat.class);
        return job;
    }

}
