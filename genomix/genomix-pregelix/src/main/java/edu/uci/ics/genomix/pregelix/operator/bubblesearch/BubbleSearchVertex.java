package edu.uci.ics.genomix.pregelix.operator.bubblesearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.bubblesearch.BubbleSearchMessage.MessageType;
import edu.uci.ics.genomix.pregelix.operator.bubblesearch.BubbleSearchMessage.NodeInfo;
import edu.uci.ics.pregelix.api.job.PregelixJob;

import java.util.Random;

public class BubbleSearchVertex extends DeBruijnGraphCleanVertex<BubbleSearchValue, BubbleSearchMessage> {
	
	private static final double MIN_SIMILARITY = .95;
	private static final int MAX_BRANCH_LENGTH = 100;
	private static final int MAX_ITERATIONS = 25;
	private static final int MAX_BRANCHES = 100000;

	private boolean isStartSeed() {
//		return getVertexId().equals(new VKmer("CCCCCCCCCCCGTCCGCCCCC"));
//		return getVertexValue().degree(DIR.FORWARD) > 1 && new Random().nextFloat() < .1;
//		return getVertexValue().degree(DIR.FORWARD) > 1 && getVertexValue().getKmerLength() > 35;
		return getVertexValue().degree(DIR.FORWARD) > 1;
	}

	@Override
	public void compute(Iterator<BubbleSearchMessage> msgIterator) throws Exception {
		BubbleSearchValue vertex = getVertexValue();
		if (getSuperstep() == 1 && isStartSeed()) {
			BubbleSearchMessage msg = new BubbleSearchMessage();
			msg.type = MessageType.EXPAND_PATH;	
			msg.outgoingET = EDGETYPE.FF;
			msg.seed = new VKmer(getVertexId());
			msgIterator = Collections.singleton(msg).iterator();
			LOG.info("Starting bubblesearch seed at " + getVertexId());
		}
		
		ArrayList<BubbleSearchMessage> completePaths = new ArrayList<>();
		while(msgIterator.hasNext()) {
			BubbleSearchMessage msg = msgIterator.next();
			switch(msg.type) {
			case EXPAND_PATH:
				msg.path.add(new NodeInfo(new VKmer(getVertexId()), new VKmer(vertex.getInternalKmer()), msg.outgoingET, vertex.getAverageCoverage()));
				if (vertex.degree(msg.outgoingET.neighborDir()) == 0 || getSuperstep() > MAX_ITERATIONS || kmerLength(msg.path) > MAX_BRANCH_LENGTH) {
					msg.type = MessageType.COMPLETE_PATH;
					sendMsg(msg.seed, msg);
//					LOG.info("Path completed at " + getVertexId() + " with " + msg);
					continue;
				}
				msg.type = MessageType.EXPAND_PATH;
				int numNeighbors = 0;
				for (EDGETYPE et : msg.outgoingET.neighborDir().edgeTypes()) {
					for (VKmer neighbor : vertex.getEdges(et)) {
						msg.outgoingET = et;
						sendMsg(neighbor, msg);
						numNeighbors++;
					}
				}
				if (numNeighbors > 1) {
					outgoingMsg.reset();
					outgoingMsg.type = MessageType.ADDITIONAL_BRANCHES;
					outgoingMsg.additionalBranches = numNeighbors - 1;
					sendMsg(msg.seed, outgoingMsg);
				}
				break;
			case ADDITIONAL_BRANCHES:
				vertex.totalBranches += msg.additionalBranches;
//				LOG.info("Tracking " + msg.additionalBranches + " more branches");
				break;
			case COMPLETE_PATH:
				completePaths.add(msg);
				break;
			case PRUNE_EDGE:
//				vertex.edgesToRemove.add(new ImmutablePair<>(msg.path.get(0).incomingET, msg.path.get(0).nodeId));
				vertex.getEdges(msg.path.get(0).incomingET).remove(msg.path.get(0).nodeId, true);
				LOG.info("Removing reciprocal bubble start " + msg.path.get(0));
				break;
			}
		}
		
		handleCompletePaths(completePaths);
		voteToHalt();
	}
	
	private void handleCompletePaths(ArrayList<BubbleSearchMessage> completePaths) {
		BubbleSearchValue vertex = getVertexValue();
		if (vertex.lastIterationSeen != getSuperstep()) {
			vertex.numCompleteThisIteration = 0;
			vertex.lastIterationSeen = (int) getSuperstep();
		}
		vertex.numCompleteThisIteration += completePaths.size();
		boolean finishAnyway = getSuperstep() > MAX_ITERATIONS + 3 || vertex.numCompleteThisIteration > MAX_BRANCHES;
		if (vertex.numCompleteThisIteration < vertex.totalBranches && !finishAnyway) {
			// resend the paths messages to myself rather than storing them (circumvent node size limits)
			for (BubbleSearchMessage msg : completePaths) {
				sendMsg(getVertexId(), msg);
			}
			if (completePaths.size() > 0) {
//				LOG.info("Resent " + completePaths.size() + " waiting paths from " + getVertexId() + ". Need " + vertex.totalBranches + " to finish. Saw " + vertex.numCompleteThisIteration + " so far this iteration.");
			}
		} else if (vertex.totalBranches > 0) {
			HashSet<Pair<EDGETYPE, VKmer>> edgesToRemove = new HashSet<>();
			// we have a complete set of possible bubbles. For similar bubbles that don't share the first edge, remove the edge with less average coverage
			// TODO: care about coverage in conflicting cases
			for (int i=0; i < completePaths.size(); i++) {
				List<NodeInfo> pathI = completePaths.get(i).path;
				if (pathI.size() < 2) {
					continue;
				}
				for (int j=i + 1; j < completePaths.size(); j++) {
					List<NodeInfo> pathJ = completePaths.get(j).path;
					if (pathJ.size() < 2) {
						continue;
					}
					if (pathI.get(1).nodeId.equals(pathJ.get(1).nodeId)) {
						continue;
					}
					List<Pair<Integer, Integer>> bubbles = findBubbles(pathI, pathJ);
					for (Pair<Integer, Integer> endpoints : bubbles) {
						List<NodeInfo> uncommonI = pathI.subList(1, endpoints.getLeft());
						List<NodeInfo> uncommonJ = pathJ.subList(1, endpoints.getRight());
						if (uncommonI.size() > 0 && uncommonJ.size() > 0 && similarKmers(uncommonI, uncommonJ)) {
//							LOG.info("Found similar kmers " + mergedKmer(uncommonI) + " vs " + mergedKmer(uncommonJ));
							float coverageI = coverage(uncommonI);
							float coverageJ = coverage(uncommonJ);
							if (coverageI < coverageJ) {
//								vertex.edgesToRemove.add(new ImmutablePair<>(uncommonI.get(0).incomingET, uncommonI.get(0).nodeId));
								edgesToRemove.add(new ImmutablePair<>(uncommonI.get(0).incomingET, uncommonI.get(0).nodeId));
							} else {
//								vertex.edgesToRemove.add(new ImmutablePair<>(uncommonJ.get(0).incomingET, uncommonJ.get(0).nodeId));
								edgesToRemove.add(new ImmutablePair<>(uncommonJ.get(0).incomingET, uncommonJ.get(0).nodeId));
							}
						}
					}
				}
			}
			// remove the requested edges
//			for (Pair<EDGETYPE, VKmer> toRemove : vertex.edgesToRemove) {
			for (Pair<EDGETYPE, VKmer> toRemove : edgesToRemove) {
				vertex.getEdges(toRemove.getLeft()).remove(toRemove.getRight(), true);
				outgoingMsg.reset();
				outgoingMsg.type = MessageType.PRUNE_EDGE;
				outgoingMsg.path.add(new NodeInfo(getVertexId(), new VKmer(), toRemove.getLeft().mirror(), 0));
				sendMsg(toRemove.getRight(), outgoingMsg);
				LOG.info("Removing bubble start " + toRemove);
			}
//			vertex.edgesToRemove.clear();
			edgesToRemove.clear();
		}
	}
	
	// returns a list of endpoints of bubbles between the two given paths
	private List<Pair<Integer, Integer>> findBubbles(List<NodeInfo> pathI, List<NodeInfo> pathJ) {
		ArrayList<Pair<Integer, Integer>> allBubbles = new ArrayList<>();
		for (int i=1; i < pathI.size(); i++) {
			for (int j=1; j < pathJ.size(); j++) {
				if (pathI.get(i).nodeId.equals(pathJ.get(j).nodeId)) {
					allBubbles.add(new ImmutablePair<>(i, j));
				}
			}
		}
		return allBubbles;
	}

	private boolean similarKmers(List<NodeInfo> uncommonI, List<NodeInfo> uncommonJ) {
		VKmer kmerI = mergedKmer(uncommonI);
		VKmer kmerJ = mergedKmer(uncommonJ);
		return (1d - kmerI.fracDissimilar(uncommonI.get(0).incomingET.causesFlip() ^ uncommonJ.get(0).incomingET.causesFlip(), kmerJ)) < MIN_SIMILARITY;
	}

	private VKmer mergedKmer(List<NodeInfo> path) {
		VKmer merged = new VKmer(path.get(0).nodeSeq);
		for (int i=1; i < path.size(); i++) {
			merged.mergeWithKmerInDir(path.get(i).incomingET, kmerSize, path.get(i).nodeSeq);
		}
		return merged;
	}

	private float coverage(List<NodeInfo> path) {
		float coverage = (float) path.get(0).coverage;
		int length = path.get(0).nodeSeq.getKmerLetterLength();
		for (int i=1; i < path.size(); i++) {
			coverage = Node.getMergedCoverage(length, coverage, path.get(i).nodeSeq.getKmerLetterLength(), path.get(i).coverage);
			length += path.get(i).nodeSeq.getKmerLetterLength() - kmerSize + 1;
		}
		return coverage;
	}
	
	public static int kmerLength(ArrayList<NodeInfo> path) {
		int length = path.get(0).nodeSeq.getKmerLetterLength();
		for (int i=1; i < path.size(); i++) {
			length += path.get(i).nodeSeq.getKmerLetterLength() - kmerSize + 1;
		}
		return length;
	}

	@Override
	public void configure(Configuration conf) {
		super.configure(conf);
		outgoingMsg = new BubbleSearchMessage();
	}
	
    public static PregelixJob getConfiguredJob(
            GenomixJobConf conf,
            Class<? extends DeBruijnGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass)
            throws IOException {
        PregelixJob job = DeBruijnGraphCleanVertex.getConfiguredJob(conf, vertexClass);
        job.setVertexInputFormatClass(NodeToBubbleSearchVertexInputFormat.class);
        job.setVertexOutputFormatClass(BubbleSearchVertexToNodeOutputFormat.class);
        return job;
    }

}
