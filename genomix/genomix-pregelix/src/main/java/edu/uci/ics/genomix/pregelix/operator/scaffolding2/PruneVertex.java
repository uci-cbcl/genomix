package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.scaffolding2.RayScores.Rules;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class PruneVertex extends DeBruijnGraphCleanVertex<RayValue, PruneMessage> {

	public static boolean SAVE_BEST_PATH;
	@Override
	public void configure(Configuration conf) {
		// Unlike every other job, we DON'T want to clear state from the previous job.
		outgoingMsg = new PruneMessage();
		SAVE_BEST_PATH = Boolean.parseBoolean(conf.get(GenomixJobConf.SCAFFOLDING_SAVE_BEST_PATH));
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
	public void open() {
		verbose = true;
	}
	
	@Override
	public void compute(Iterator<PruneMessage> msgIterator) throws Exception {
		PruneMessage msg;
		RayValue vertex = getVertexValue();
		if (getSuperstep() == 1) {
			// notify neighbors about which edges I want to keep
			for (Entry<Entry<EDGETYPE, VKmer>, Rules> path : vertex.getOutgoingEdgesToKeep().entrySet()) {
				Entry<EDGETYPE, VKmer> entry = path.getKey();
				// send to my neighbor the scores I want to keep
				outgoingMsg.reset();
				outgoingMsg.setSourceVertexId(getVertexId());
				outgoingMsg.setFlag(entry.getKey().mirror().get());
				outgoingMsg.rules = path.getValue();
				sendMsg(entry.getValue(), outgoingMsg);
				
				// also send a msg to myself indicating I want to keep this edge to my neighbor
				outgoingMsg.reset();
				outgoingMsg.setSourceVertexId(entry.getValue());
				outgoingMsg.setFlag(entry.getKey().get());
				outgoingMsg.rules = path.getValue();
				sendMsg(getVertexId(), outgoingMsg);
			}
			
			for (Entry<Entry<EDGETYPE, VKmer>, Rules> path : vertex.getIncomingEdgesToKeep().entrySet()) {
				Entry<EDGETYPE, VKmer> entry = path.getKey();
				// to neighbor
				outgoingMsg.reset();
				outgoingMsg.setSourceVertexId(getVertexId());
				outgoingMsg.setFlag(entry.getKey().mirror().get());
				outgoingMsg.rules = path.getValue();
				sendMsg(entry.getValue(), outgoingMsg);
				
				// to self
				outgoingMsg.reset();
				outgoingMsg.setSourceVertexId(entry.getValue());
				outgoingMsg.setFlag(entry.getKey().get());
				outgoingMsg.rules = path.getValue();
				sendMsg(getVertexId(), outgoingMsg);
			}
			
			// reset the edges we just kept about
			vertex.incomingEdgesToKeep = null;
			vertex.outgoingEdgesToKeep = null;
		} else if (getSuperstep() == 2) {
			// collect neighboring "keep" nodes and prune the rest.  Then tell neighbors to prune back edges
			HashMap<Entry<EDGETYPE, VKmer>, Rules> FneighborEdgesToKeep = new HashMap<>();
			HashMap<Entry<EDGETYPE, VKmer>, Rules> RneighborEdgesToKeep = new HashMap<>();
			while(msgIterator.hasNext()) {
				msg = msgIterator.next();
				SimpleEntry<EDGETYPE, VKmer> key = new SimpleEntry<>(EDGETYPE.fromByte(msg.getFlag()), new VKmer(msg.getSourceVertexId()));
				Rules value = msg.rules;
				if (key.getKey().dir() == DIR.FORWARD) {
					if (!FneighborEdgesToKeep.containsKey(key) || FneighborEdgesToKeep.get(key) == null || (value != null && FneighborEdgesToKeep.get(key).ruleC < value.ruleC)) {
						FneighborEdgesToKeep.put(key, value);
					}
				} else if (!RneighborEdgesToKeep.containsKey(key) || RneighborEdgesToKeep.get(key) == null || (value != null && RneighborEdgesToKeep.get(key).ruleC < value.ruleC)) {
					RneighborEdgesToKeep.put(key, value);
				}
			}
			if (verbose) {
				if (FneighborEdgesToKeep.size() > 1 || RneighborEdgesToKeep.size() > 1) {
					LOG.info("Found an intersection where we disagree about the correct path! " + getVertexId() + " with FtoKeep: " + FneighborEdgesToKeep + ", RtoKeep: " + RneighborEdgesToKeep);
				}
			}
			Set<Entry<EDGETYPE, VKmer>> prunedEdges = pruneUnsavedEdges(FneighborEdgesToKeep, RneighborEdgesToKeep);
			if (verbose && prunedEdges.size() > 0) {
				LOG.info("Pruned " + prunedEdges + " from FtoKeep: " + FneighborEdgesToKeep + ", RtoKeep: " + RneighborEdgesToKeep);
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
		} else {
			// Respond to a prune edge request.
			while (msgIterator.hasNext()) {
				msg = msgIterator.next();
				if (verbose) {
					LOG.info("in " + getVertexId() + ", request to prune back edge: " + msg.getSourceVertexId() + " in my " + EDGETYPE.fromByte(msg.getFlag()));
				}
				vertex.getEdges(EDGETYPE.fromByte(msg.getFlag())).remove(msg.getSourceVertexId(), true);
			}
		}
		voteToHalt();
	}

	private Set<Entry<EDGETYPE, VKmer>> pruneUnsavedEdges(HashMap<Entry<EDGETYPE, VKmer>, Rules> FneighborEdgesToKeep, HashMap<Entry<EDGETYPE, VKmer>, Rules> RneighborEdgesToKeep) {
		RayValue vertex = getVertexValue();
		HashSet<Entry<EDGETYPE, VKmer>> prunedEdges = new HashSet<>();
		// keep only the max ruleC score
		int maxF = 0;
		for (Rules e : FneighborEdgesToKeep.values()) {
			maxF = Math.max(maxF, e != null ? e.ruleC : 0);
		}
		int maxR = 0;
		for (Rules e : RneighborEdgesToKeep.values()) {
			maxR = Math.max(maxR, e != null ? e.ruleC : 0);
		}

		if (SAVE_BEST_PATH) {
			if (maxF != 0) {
				for (EDGETYPE et : DIR.FORWARD.edgeTypes()) {
					Iterator<VKmer> it = vertex.getEdges(et).iterator();
					while (it.hasNext()) {
						VKmer kmer = it.next();
						SimpleEntry<EDGETYPE, VKmer> entry = new SimpleEntry<>(et, new VKmer(kmer));
						if (!FneighborEdgesToKeep.containsKey(entry) || FneighborEdgesToKeep.get(entry) == null || FneighborEdgesToKeep.get(entry).ruleC < maxF) {
							it.remove();
							prunedEdges.add(entry);
						}
					}
				}
			}
			if (maxR != 0) {
				for (EDGETYPE et : DIR.REVERSE.edgeTypes()) {
					Iterator<VKmer> it = vertex.getEdges(et).iterator();
					while (it.hasNext()) {
						VKmer kmer = it.next();
						SimpleEntry<EDGETYPE, VKmer> entry = new SimpleEntry<>(et, new VKmer(kmer));
						if (!RneighborEdgesToKeep.containsKey(entry) || RneighborEdgesToKeep.get(entry) == null || RneighborEdgesToKeep.get(entry).ruleC < maxR) {
							it.remove();
							prunedEdges.add(entry);
						}
					}
				}	
			}
		} else {
			if (FneighborEdgesToKeep.size() > 0) {
				for (EDGETYPE et : DIR.FORWARD.edgeTypes()) {
					Iterator<VKmer> it = vertex.getEdges(et).iterator();
					while (it.hasNext()) {
						VKmer kmer = it.next();
						SimpleEntry<EDGETYPE, VKmer> entry = new SimpleEntry<>(et, new VKmer(kmer));
						if (!FneighborEdgesToKeep.containsKey(entry)) {
							it.remove();
							prunedEdges.add(entry);
						}
					}
				}
			}
			if (RneighborEdgesToKeep.size() > 0) {
				for (EDGETYPE et : DIR.REVERSE.edgeTypes()) {
					Iterator<VKmer> it = vertex.getEdges(et).iterator();
					while (it.hasNext()) {
						VKmer kmer = it.next();
						SimpleEntry<EDGETYPE, VKmer> entry = new SimpleEntry<>(et, new VKmer(kmer));
						if (!RneighborEdgesToKeep.containsKey(entry)) {
							it.remove();
							prunedEdges.add(entry);
						}
					}
				}
			}
		}
		return prunedEdges;
	}
}
