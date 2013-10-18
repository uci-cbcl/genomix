package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.type.Node.EDGETYPE;
import edu.uci.ics.genomix.type.Node.READHEAD_ORIENTATION;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

public class BFSTraverseMessage extends MessageWritable {

    private long readId; //use for BFSTravese
    private VKmerList pathList; //use for BFSTravese
    private ArrayListWritable<EDGETYPE> edgeTypeList; //use for BFSTravese
    private VKmer seekedVertexId; //use for BFSTravese
    private READHEAD_ORIENTATION srcReadHeadOrientation; //use for BFSTravese
    private READHEAD_ORIENTATION destReadHeadOrientation; //use for BFSTravese
    private boolean isTraverseMsg; //otherwise, it is final message for this path for adding readId to all path nodes
    private int totalBFSLength;

    public BFSTraverseMessage() {
        super();
        pathList = new VKmerList();
        edgeTypeList = new ArrayListWritable<EDGETYPE>();
        seekedVertexId = new VKmer();
        readId = 0;
        srcReadHeadOrientation = READHEAD_ORIENTATION.UNFLIPPED;
        destReadHeadOrientation = READHEAD_ORIENTATION.UNFLIPPED;
        isTraverseMsg = true;
        totalBFSLength = 0;
    }

    public void reset() {
        super.reset();
        pathList.reset();
        edgeTypeList.clear();
        seekedVertexId.reset(0);
        readId = 0;
        srcReadHeadOrientation = READHEAD_ORIENTATION.UNFLIPPED;
        destReadHeadOrientation = READHEAD_ORIENTATION.UNFLIPPED;
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

    public VKmer getSeekedVertexId() {
        return seekedVertexId;
    }

    public void setSeekedVertexId(VKmer seekedVertexId) {
        this.seekedVertexId.setAsCopy(seekedVertexId);
    }

    public long getReadId() {
        return readId;
    }

    public void setReadId(long readId) {
        this.readId = readId;
    }

    public READHEAD_ORIENTATION getSrcReadHeadOrientation() {
        return srcReadHeadOrientation;
    }

    public void setSrcReadHeadOrientation(READHEAD_ORIENTATION srcReadHeadOrientation) {
        this.srcReadHeadOrientation = srcReadHeadOrientation;
    }

    public READHEAD_ORIENTATION getDestReadHeadOrientation() {
        return destReadHeadOrientation;
    }

    public void setDestReadHeadOrientation(READHEAD_ORIENTATION destReadHeadOrientation) {
        this.destReadHeadOrientation = destReadHeadOrientation;
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
        seekedVertexId.readFields(in);
        readId = in.readLong();
        srcReadHeadOrientation = READHEAD_ORIENTATION.fromByte(in.readByte());
        destReadHeadOrientation = READHEAD_ORIENTATION.fromByte(in.readByte());
        isTraverseMsg = in.readBoolean();
        totalBFSLength = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        pathList.write(out);
        edgeTypeList.write(out);
        seekedVertexId.write(out);
        out.writeLong(readId);
        out.writeByte(srcReadHeadOrientation.get());
        out.writeByte(destReadHeadOrientation.get());
        out.writeBoolean(isTraverseMsg);
        out.writeInt(totalBFSLength);
    }
}
