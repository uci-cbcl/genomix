package edu.uci.ics.genomix.pregelix.operator.complexbubblemerge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.EdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.vertex.VertexValueWritable;

public class BubbleMergeWithSearchVertexValueWritable extends VertexValueWritable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public static class BubbleMergeWithSearchState extends State {

        // mark if BFS passes this vertex in bubbleMergeWithSearch
        public static final byte SEARCH_THROUGH = 0b1 << 3;

        public static final byte SEARCH_THROUGH_MASK = 0b1 << 3;

        // UPDATE_PATH_TO_NEXT, END_NOTICE_TO_SRC and UPDATE_BRANCH_TO_SRC are used in BubbleMergeWithSearch
        public static final byte UPDATE_PATH_IN_NEXT = 0b001 << 4;
        public static final byte END_NOTICE_IN_SRC = 0b010 << 4;
        public static final byte UPDATE_BRANCH_IN_SRC = 0b011 << 4;
        public static final byte SAVE_ONLY_PATH_NODES = 0b100 << 4;
        public static final byte SAVE_ONLY_PATH_NODES_LAST_NODE = 0b101 << 4;
        public static final byte PRUNE_DEAD_EDGE = 0b110 << 4;

        public static final byte BUBBLE_WITH_SEARCH_FLAG_MASK = 0b111 << 4;

    }

    private ArrayListWritable<VKmerList> arrayOfPathList;
    private ArrayListWritable<VKmer> arrayOfInternalKmer;
    private ArrayListWritable<EdgeTypeList> arrayOfEdgeTypes;
    private Integer numBranches;

    public BubbleMergeWithSearchVertexValueWritable() {
        super();
        arrayOfPathList = new ArrayListWritable<VKmerList>();
        arrayOfInternalKmer = new ArrayListWritable<VKmer>();
        arrayOfEdgeTypes = new ArrayListWritable<EdgeTypeList>();
        numBranches = 0;
    }

    public void setAsCopy(BubbleMergeWithSearchVertexValueWritable other) {
        super.setAsCopy(other);
    }

    public ArrayList<VKmerList> getArrayOfPathList() {
        return arrayOfPathList;
    }

    public void setArrayOfPathList(ArrayList<VKmerList> arrayOfPathList) {
        this.arrayOfPathList.clear();
        this.arrayOfPathList.addAll(arrayOfPathList);
    }

    public ArrayList<VKmer> getArrayOfInternalKmer() {
        return arrayOfInternalKmer;
    }

    public void setArrayOfInternalKmer(ArrayList<VKmer> arrayOfInternalKmer) {
        this.arrayOfInternalKmer.clear();
        this.arrayOfInternalKmer.addAll(arrayOfInternalKmer);
    }

    public ArrayListWritable<EdgeTypeList> getArrayOfEdgeTypes() {
        return arrayOfEdgeTypes;
    }

    public void setArrayOfEdgeTypes(ArrayListWritable<EdgeTypeList> arrayOfEdgeType) {
        this.arrayOfEdgeTypes.clear();
        this.arrayOfEdgeTypes.addAll(arrayOfEdgeType);
    }

    public Integer getNumBranches() {
        return numBranches;
    }

    public void setNumBranches(Integer numBranches) {
        this.numBranches = numBranches;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        this.arrayOfPathList.readFields(in);
        this.arrayOfInternalKmer.readFields(in);
        this.arrayOfEdgeTypes.readFields(in);
        numBranches = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        this.arrayOfPathList.write(out);
        this.arrayOfInternalKmer.write(out);
        this.arrayOfEdgeTypes.write(out);
        out.writeInt(numBranches);
    }
}
