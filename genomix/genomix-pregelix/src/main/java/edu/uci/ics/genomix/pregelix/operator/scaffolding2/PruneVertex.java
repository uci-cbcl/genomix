package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class PruneVertex extends DeBruijnGraphCleanVertex<RayValue, MessageWritable> {

	@Override
	public void configure(Configuration conf) {
		// Unlike every other job, we DON'T want to clear state from the previous job.
		outgoingMsg = new MessageWritable();
	}
	
	public static PregelixJob getConfiguredJob(
            GenomixJobConf conf,
            Class<? extends DeBruijnGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass)
            throws IOException {
        PregelixJob job = DeBruijnGraphCleanVertex.getConfiguredJob(conf, vertexClass);
        job.setVertexInputFormatClass(NodeToRayVertexInputFormat.class);
        job.setVertexOutputFormatClass(RayVertexToNodeOutputFormat.class);
        return job;
    }
	
	@Override
	public void compute(Iterator<MessageWritable> msgIterator) throws Exception {
		MessageWritable msg;
		RayValue vertex = getVertexValue();
		if (getSuperstep() == 1) {
			// notify neighbors about which edges I want to keep
			for (Entry<EDGETYPE, VKmer> entry : vertex.getOutgoingEdgesToKeep()) {
				outgoingMsg.reset();
				outgoingMsg.setSourceVertexId(getVertexId());
				outgoingMsg.setFlag(entry.getKey().mirror().get());
				sendMsg(entry.getValue(), outgoingMsg);
			}
			for (Entry<EDGETYPE, VKmer> entry : vertex.getIncomingEdgesToKeep()) {
				outgoingMsg.reset();
				outgoingMsg.setSourceVertexId(getVertexId());
				outgoingMsg.setFlag(entry.getKey().mirror().get());
				sendMsg(entry.getValue(), outgoingMsg);
			}
		} else if (getSuperstep() == 2) {
			// collect neighboring "keep" nodes and prune the rest.  Then tell neighbors to prune back edges
			Set<Entry<EDGETYPE, VKmer>> neighborEdgesToKeep = new HashSet<>();
			while(msgIterator.hasNext()) {
				msg = msgIterator.next();
				neighborEdgesToKeep.add(new SimpleEntry<>(EDGETYPE.fromByte(msg.getFlag()), new VKmer(msg.getSourceVertexId())));
			}
			Set<Entry<EDGETYPE, VKmer>> prunedEdges = pruneUnsavedEdges(neighborEdgesToKeep);
			if (verbose && prunedEdges.size() > 0) {
				LOG.info("Pruned " + prunedEdges);
			}
			for (Entry<EDGETYPE, VKmer> entry : prunedEdges) {
				outgoingMsg.reset();
				outgoingMsg.setSourceVertexId(getVertexId());
				outgoingMsg.setFlag(entry.getKey().mirror().get());
				sendMsg(entry.getValue(), outgoingMsg);
				if (verbose) {
					LOG.info("Telling " + entry.getValue() + " in my " + entry.getKey() + " to prune me (" + getVertexId() + ").");	
				}
			}
			voteToHalt();
		} else {
			// Respond to a prune edge request.
			while (msgIterator.hasNext()) {
				msg = msgIterator.next();
				if (verbose) {
					LOG.info("in " + getVertexId() + ", request to prune back edge: " + msg.getSourceVertexId() + " in my " + EDGETYPE.fromByte(msg.getFlag()));
				}
				vertex.getEdges(EDGETYPE.fromByte(msg.getFlag())).remove(msg.getSourceVertexId(), true);
			}
			voteToHalt();
		}
	}

	private Set<Entry<EDGETYPE, VKmer>> pruneUnsavedEdges(Set<Entry<EDGETYPE, VKmer>> neighborEdgesToKeep) {
		RayValue vertex = getVertexValue();
		VKmer kmer;
		SimpleEntry<EDGETYPE, VKmer> entry;
		HashSet<Entry<EDGETYPE, VKmer>> prunedEdges = new HashSet<>();
		if (RayVertex.REMOVE_OTHER_OUTGOING && vertex.getOutgoingEdgesToKeep().size() > 0) {
			for (EDGETYPE et : EDGETYPE.values) {
				Iterator<VKmer> edges = vertex.getEdges(et).iterator();
				while(edges.hasNext()) {
					kmer = edges.next();
					entry = new SimpleEntry<>(et, new VKmer(kmer));
					if (!neighborEdgesToKeep.contains(entry) && !vertex.getOutgoingEdgesToKeep().contains(entry)) {
						prunedEdges.add(entry);
						edges.remove();
					}
				}
			}
		}
		if (RayVertex.REMOVE_OTHER_INCOMING && vertex.getIncomingEdgesToKeep().size() > 0) {
			for (EDGETYPE et : EDGETYPE.values) {
				Iterator<VKmer> edges = vertex.getEdges(et).iterator();
				while(edges.hasNext()) {
					kmer = edges.next();
					entry = new SimpleEntry<>(et, new VKmer(kmer));
					if (!neighborEdgesToKeep.contains(entry) && !vertex.getIncomingEdgesToKeep().contains(entry)) {
						prunedEdges.add(entry);
						edges.remove();
					}
				}
			}
		}
		return prunedEdges;
	}
}
