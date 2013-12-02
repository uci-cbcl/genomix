package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.EdgeTypeList;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

public class BubbleMergeWithSearchVertexValueWritable extends VertexValueWritable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

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
