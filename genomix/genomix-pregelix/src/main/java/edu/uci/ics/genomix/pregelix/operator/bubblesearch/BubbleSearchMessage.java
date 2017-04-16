package edu.uci.ics.genomix.pregelix.operator.bubblesearch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
		public void write(DataOutput out) throws IOException {
			nodeId.write(out);
			nodeSeq.write(out);
			out.writeByte(incomingET.get());
			out.writeFloat(coverage);
		}
		public static NodeInfo newFromStream(DataInput in) throws IOException {
			VKmer id = new VKmer(), seq = new VKmer();
			id.readFields(in);
			seq.readFields(in);
			EDGETYPE et = EDGETYPE.fromByte(in.readByte());
			float coverage = in.readFloat();
			return new NodeInfo(id, seq, et, coverage);
		}
		public String toString() {
			return "{inET:" + incomingET + ", cov:" + coverage +", id:" + nodeId + ", seq:" + nodeSeq + "}"; 
		}
	}
	public enum MessageType {
		EXPAND_PATH(0),
		COMPLETE_PATH(1),
		ADDITIONAL_BRANCHES(2),
		PRUNE_EDGE(3), 
		PRUNE_NODE(4);
		private byte value;
		private MessageType(int value) {
			this.value = (byte) value;
		}
		public static MessageType fromByte(byte value) {
			switch(value) {
			case 0: return EXPAND_PATH;
			case 1: return COMPLETE_PATH;
			case 2: return ADDITIONAL_BRANCHES;
			case 3: return PRUNE_EDGE;
			case 4: return PRUNE_NODE;
			default: throw new IllegalArgumentException("Unknown byte value: " + value);
			}
		}
		public byte toByte() {
			return value;
		}
	}

	public ArrayList<NodeInfo> path = new ArrayList<>();
	public MessageType type;
	public EDGETYPE outgoingET;  // The ET from the last node in path towards the next node
	public VKmer seed = new VKmer(); 
	public int additionalBranches;
	
	@Override
	public String toString() {
		return "<msg type: " + type + ", outgoingET:" + outgoingET + ", seed:" + seed + ", addBranches:" + additionalBranches + ", path: size=" + path.size() + ",length=" + BubbleSearchVertex.kmerLength(path) + "nodes=" + path + ">";  
		
	}

	@Override
	public void reset() {
		super.reset();
		path.clear();
		type = MessageType.EXPAND_PATH;
		outgoingET = EDGETYPE.FF;
		seed = new VKmer();
		additionalBranches = 0;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(path.size());
		for (NodeInfo p : path) {
			p.write(out);
		}
		out.writeByte(type.toByte());
		out.writeByte(outgoingET.get());
		seed.write(out);
		out.writeInt(additionalBranches);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		int count = in.readInt();
		for (int i=0; i < count; i++) {
			path.add(NodeInfo.newFromStream(in));
		}
		type = MessageType.fromByte(in.readByte());
		outgoingET = EDGETYPE.fromByte(in.readByte());
		seed.readFields(in);
		additionalBranches = in.readInt();
	}
	
	@Override
	public int sizeInBytes() {
		int sizeInBytes = super.sizeInBytes();
		sizeInBytes += 1 + 1 + seed.getKmerByteLength() + Integer.SIZE / 8;
		for (NodeInfo p : path) {
			sizeInBytes += Float.SIZE / 8 + 1 + p.nodeId.getKmerByteLength() + p.nodeSeq.getKmerByteLength();
		}
		return sizeInBytes;
	}
}
