package edu.uci.ics.genomix.pregelix.operator.bubblesearch;

import java.util.ArrayList;

import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;

public class BubbleSearchMessage extends MessageWritable {

	public static class NodeInfo {

		public VKmer nodeId;
		public VKmer nodeSeq;
		public EDGETYPE incomingET;
		public float coverage;

		public NodeInfo(VKmer nodeId, VKmer nodeSeq, EDGETYPE incomingET, float coverage) {
			this.nodeId = nodeId;
			this.nodeSeq = nodeSeq;
			this.incomingET = incomingET;
			this.coverage = coverage;
		}
	}
	public enum MessageType {
		EXPAND_PATH,
		COMPLETE_PATH,
		ADDITIONAL_BRANCHES,
		PRUNE_EDGE;
	}

	public ArrayList<NodeInfo> path = new ArrayList<>();
	public MessageType type;
	public EDGETYPE outgoingET;  // The ET from the last node in path towards the next node
	public VKmer seed;
	public int additionalBranches;

}
