package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

public class BFSTraverseMessage extends MessageWritable {

    private long readId; //use for BFSTravese
    private VKmerList pathList; //use for BFSTravese
    private ArrayListWritable<EDGETYPE> edgeTypeList; //use for BFSTravese
    private VKmer targetVertexId; //use for BFSTravese
    private boolean srcFlip; //use for BFSTravese
    private boolean destFlip; //use for BFSTravese
    private boolean isTraverseMsg; //otherwise, it is final message for this path for adding readId to all path nodes
    private int totalBFSLength;

    public BFSTraverseMessage() {
        super();
        pathList = new VKmerList();
        edgeTypeList = new ArrayListWritable<EDGETYPE>();
        targetVertexId = new VKmer();
        readId = 0;
        srcFlip = false;
        destFlip = false;
        isTraverseMsg = true;
        totalBFSLength = 0;
    }

    public void reset() {
        super.reset();
        pathList.reset();
        edgeTypeList.clear();
        targetVertexId.reset(0);
        readId = 0;
        srcFlip = false;
        destFlip = false;
        isTraverseMsg = true;
        totalBFSLength = 0;
    }

    public VKmerList getPathList() {
        return pathList;
    }

    public void setPathList(VKmerList pathList) {
        this.pathList = pathList;
    }

    public ArrayListWritable<EDGETYPE> getEdgeTypeList() {
        return edgeTypeList;
    }

    public void setEdgeTypeList(ArrayListWritable<EDGETYPE> edgeDirsList) {
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

    public boolean isTraverseMsg() {
        return isTraverseMsg;
    }

    public void setTraverseMsg(boolean isTraverseMsg) {
        this.isTraverseMsg = isTraverseMsg;
    }
    
    public int getTotalBFSLength() {
        return totalBFSLength;
    }

    public void setTotalBFSLength(int totalBFSLength) {
        this.totalBFSLength = totalBFSLength;
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
        isTraverseMsg = in.readBoolean();
        totalBFSLength = in.readInt();
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
        out.writeBoolean(isTraverseMsg);
        out.writeInt(totalBFSLength);
    }
}
