package edu.uci.ics.genomix.pregelix.extractsubgraph;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.format.ExtractSubgraphOutputFormat;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class ExtractSubgraphVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable> {
    public static final String START_SEEDS = "ExtractSubgraphVertex.startSeeds";
    public static final String NUM_HOPS = "ExtractSubgraphVertex.numOfHops";

    private Set<VKmer> startSeeds = null;
    private int numOfHops = -1;

    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (outgoingMsg == null)
            outgoingMsg = new MessageWritable();
        if (startSeeds == null) {
            startSeeds = new HashSet<VKmer>();
            if (getContext().getConfiguration().get(START_SEEDS) != null) {
                for (String kmer : getContext().getConfiguration().get(START_SEEDS).split(",")) {
                    startSeeds.add(new VKmer(kmer));
                }
            }
        }
        if (numOfHops == -1) {
            numOfHops = Integer.parseInt(getContext().getConfiguration().get(NUM_HOPS));
        }
    }

    public void markSelfAndBoardcast() {
        VertexValueWritable vertex = getVertexValue();
        vertex.setState(State.IS_MARK);
        sendSettledMsgToAllNeighborNodes(vertex);
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) throws Exception {
        initVertex();
        if (getSuperstep() == 1 && startSeeds.contains(getVertexId())) {
            markSelfAndBoardcast();
        } else if (getSuperstep() <= numOfHops + 1 && msgIterator.hasNext()) {
            markSelfAndBoardcast();
        }
        voteToHalt();
    }

    public static PregelixJob getConfiguredJob(
            GenomixJobConf conf,
            Class<? extends DeBruijnGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass)
            throws IOException {
        PregelixJob job = DeBruijnGraphCleanVertex.getConfiguredJob(conf, vertexClass);
        job.setVertexOutputFormatClass(ExtractSubgraphOutputFormat.class);
        return job;
    }

}
