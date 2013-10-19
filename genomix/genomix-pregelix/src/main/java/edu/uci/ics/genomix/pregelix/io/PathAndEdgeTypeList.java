package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.pregelix.io.common.EdgeTypeList;
import edu.uci.ics.genomix.type.VKmerList;

/**
 * PathAndEdgeTypeList stores all the kmers along the BTS path by VKmerList
 * and all the edgeType along it by ArrayListWritable<EDGETYPE>
 * Ex. After BFSearch a path A -> B -> C -> D, kmerList: A, B, C, D and
 * edgeTypeList: EDGETYPE(A-B), EDGETYPE(B-C), EDGETYPE(C-D)
 */
public class PathAndEdgeTypeList implements Writable {
    VKmerList kmerList;
    EdgeTypeList edgeTypeList;

    public PathAndEdgeTypeList() {
        kmerList = new VKmerList();
        edgeTypeList = new EdgeTypeList();
    }

    public PathAndEdgeTypeList(VKmerList kmerList, EdgeTypeList edgeTypeList) {
        this();
        this.kmerList.setCopy(kmerList);
        this.edgeTypeList.clear();
        this.edgeTypeList.addAll(edgeTypeList);
    }

    public void reset() {
        kmerList.reset();
        edgeTypeList.clear();
    }

    public int size() {
        return kmerList.size();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        kmerList.write(out);
        edgeTypeList.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        kmerList.readFields(in);
        edgeTypeList.readFields(in);
    }

    public VKmerList getKmerList() {
        return kmerList;
    }

    public void setKmerList(VKmerList kmerList) {
        this.kmerList.setCopy(kmerList);
    }

    public EdgeTypeList getEdgeTypeList() {
        return edgeTypeList;
    }

    public void setEdgeTypeList(EdgeTypeList edgeTypeList) {
        this.edgeTypeList.clear();
        this.edgeTypeList.addAll(edgeTypeList);
    }

}