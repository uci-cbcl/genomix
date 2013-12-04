package edu.uci.ics.genomix.pregelix.operator.extractsubgraph;

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
    /**
     * start from startSeeds to do broadcast(kind of "BFS")
     * numOfHops means how far you plan to broadcast
     * ex. A -> B -> C -> D ->E ->F -> G, you specify startSeed is D and numOfHops is 2,
     * you will extract graph like B -> C -> D ->E ->F
     */
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
            if (getContext().getConfiguration().get(GenomixJobConf.PLOT_SUBGRAPH_START_SEEDS) != null) {
                for (String kmer : getContext().getConfiguration().get(GenomixJobConf.PLOT_SUBGRAPH_START_SEEDS)
                        .split(",")) {
                    startSeeds.add(new VKmer(kmer));
                }
            }
        }
        if (numOfHops == -1) {
            numOfHops = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.PLOT_SUBGRAPH_NUM_HOPS));
        }
    }

    public void markSelfAndBroadcast() {
        VertexValueWritable vertex = getVertexValue();
        vertex.setState(State.KEEP_NODE);
        sendSettledMsgToAllNeighborNodes(vertex);
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) throws Exception {
        initVertex();
        if ((getSuperstep() == 1 && startSeeds.contains(getVertexId()))
                || (getSuperstep() <= numOfHops + 1 && msgIterator.hasNext())) {
            markSelfAndBroadcast();
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
