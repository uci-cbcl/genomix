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
		// Nodes visited by a walk have some subset of their edges removed
		if (getSuperstep() == 1) {
			Set<Entry<EDGETYPE, VKmer>> prunedEdges = pruneUnsavedEdges();
			for (Entry<EDGETYPE, VKmer> entry : prunedEdges) {
				outgoingMsg.reset();
				outgoingMsg.setSourceVertexId(getVertexId());
				outgoingMsg.setFlag(entry.getKey().mirror().get());
				sendMsg(entry.getValue(), outgoingMsg);
			}
		} else {
			// Respond to a prune edge request.
			MessageWritable msg;
			RayValue vertex = getVertexValue();
			while (msgIterator.hasNext()) {
				msg = msgIterator.next();
				vertex.getEdges(EDGETYPE.fromByte(msg.getFlag())).remove(msg.getSourceVertexId());
			}
		}
		voteToHalt();
	}

	private Set<Entry<EDGETYPE, VKmer>> pruneUnsavedEdges() {
		RayValue vertex = getVertexValue();
		VKmer kmer;
		SimpleEntry<EDGETYPE, VKmer> entry;
		HashSet<Entry<EDGETYPE, VKmer>> prunedEdges = new HashSet<>();
		if (RayVertex.REMOVE_OTHER_OUTGOING) {
			for (EDGETYPE et : EDGETYPE.values) {
				Iterator<VKmer> edges = vertex.getEdges(et).iterator();
				while(edges.hasNext()) {
					kmer = edges.next();
					entry = new SimpleEntry<>(et, kmer);
					if (!vertex.getOutgoingEdgesToKeep().contains(entry)) {
						prunedEdges.add(entry);
						edges.remove();
					}
				}
			}
		}
		if (RayVertex.REMOVE_OTHER_INCOMING) {
			for (EDGETYPE et : EDGETYPE.values) {
				Iterator<VKmer> edges = vertex.getEdges(et).iterator();
				while(edges.hasNext()) {
					kmer = edges.next();
					entry = new SimpleEntry<>(et, kmer);
					if (!vertex.getIncomingEdgesToKeep().contains(entry)) {
						prunedEdges.add(entry);
						edges.remove();
					}
				}
			}
		}
		return prunedEdges;
	}
}
