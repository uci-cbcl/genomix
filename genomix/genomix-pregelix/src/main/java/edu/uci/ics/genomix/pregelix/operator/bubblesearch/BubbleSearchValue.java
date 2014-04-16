package edu.uci.ics.genomix.pregelix.operator.bubblesearch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import javax.management.ImmutableDescriptor;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;

public class BubbleSearchValue extends VertexValueWritable {
    private static final long serialVersionUID = 1L;
	public int totalBranches = 0;
//	public List<Pair<EDGETYPE, VKmer>> edgesToRemove = new ArrayList<>();
    
	@Override
	public void reset() {
		super.reset();
		totalBranches = 0;
//		edgesToRemove.clear();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(totalBranches);
//		out.writeInt(edgesToRemove.size());
//		for (Pair<EDGETYPE, VKmer> r : edgesToRemove) {
//			out.writeByte(r.getLeft().get());
//			r.getRight().write(out);
//		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		totalBranches = in.readInt();
//		int count = in.readInt();
//		for (int i=0; i < count; i++) { 
//			EDGETYPE et = EDGETYPE.fromByte(in.readByte());
//			VKmer kmer = new VKmer();
//			kmer.readFields(in);
//			edgesToRemove.add(new ImmutablePair<>(et, kmer));
//		}
	}
}
