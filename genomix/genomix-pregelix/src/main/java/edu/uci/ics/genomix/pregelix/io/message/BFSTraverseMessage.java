package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.EdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.BFSTraverseVertex.SearchInfo;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

public class BFSTraverseMessage extends MessageWritable {

    private long readId; //use for BFSTravese
    private VKmerList pathList; //use for BFSTravese
    private EdgeTypeList edgeTypeList; //use for BFSTravese
    private VKmer targetVertexId; //use for BFSTravese
    private boolean srcFlip; //use for BFSTravese
    private boolean destFlip; //use for BFSTravese
    private int totalBFSLength;
    private HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap;

    public BFSTraverseMessage() {
        super();
        pathList = new VKmerList();
        edgeTypeList = new EdgeTypeList();
        targetVertexId = new VKmer();
        readId = 0;
        srcFlip = false;
        destFlip = false;
        totalBFSLength = 0;
        scaffoldingMap = new HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>>();
    }

    public void reset() {
        super.reset();
        pathList.reset();
        edgeTypeList.clear();
        targetVertexId.reset(0);
        readId = 0;
        srcFlip = false;
        destFlip = false;
        totalBFSLength = 0;
        scaffoldingMap.clear();
    }

    public VKmerList getPathList() {
        return pathList;
    }

    public void setPathList(VKmerList pathList) {
        this.pathList = pathList;
    }

    public EdgeTypeList getEdgeTypeList() {
        return edgeTypeList;
    }

    public void setEdgeTypeList(EdgeTypeList edgeDirsList) {
        this.edgeTypeList.clear();
        this.edgeTypeList.addAll(edgeDirsList);
    }

    public VKmer getTargetVertexId() {
        return targetVertexId;
    }

    public void setTargetVertexId(VKmer targetVertexId) {
        this.targetVertexId.setAsCopy(targetVertexId);
    }

    public long getReadId() {
        return readId;
    }

    public void setReadId(long readId) {
        this.readId = readId;
    }

    public boolean isSrcFlip() {
        return srcFlip;
    }

    public void setSrcFlip(boolean srcFlip) {
        this.srcFlip = srcFlip;
    }

    public boolean isDestFlip() {
        return destFlip;
    }

    public void setDestFlip(boolean destFlip) {
        this.destFlip = destFlip;
    }

    public int getTotalBFSLength() {
        return totalBFSLength;
    }

    public void setTotalBFSLength(int totalBFSLength) {
        this.totalBFSLength = totalBFSLength;
    }
    
    public HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> getScaffoldingMap() {
        return scaffoldingMap;
    }

    public void setScaffoldingMap(HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap) {
        this.scaffoldingMap.clear();
        this.scaffoldingMap.putAll(scaffoldingMap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        pathList.readFields(in);
        edgeTypeList.readFields(in);
        targetVertexId.readFields(in);
        readId = in.readLong();
        srcFlip = in.readBoolean();
        destFlip = in.readBoolean();
        totalBFSLength = in.readInt();
        scaffoldingMap.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        pathList.write(out);
        edgeTypeList.write(out);
        targetVertexId.write(out);
        out.writeLong(readId);
        out.writeBoolean(srcFlip);
        out.writeBoolean(destFlip);
        out.writeInt(totalBFSLength);
        scaffoldingMap.write(out);
    }
}
