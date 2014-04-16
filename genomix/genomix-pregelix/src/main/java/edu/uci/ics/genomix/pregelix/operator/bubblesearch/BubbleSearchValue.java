package edu.uci.ics.genomix.pregelix.operator.bubblesearch;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;

public class BubbleSearchValue extends VertexValueWritable {
    private static final long serialVersionUID = 1L;
	public int totalBranches;
	public List<Pair<EDGETYPE, VKmer>> edgesToRemove;
    
}
