package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import edu.uci.ics.genomix.pregelix.io.SearchInfo;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.EdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.type.Node.READHEAD_ORIENTATION;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

public class BFSTraverseMessage extends MessageWritable {

    private long readId; //use for BFSTravese
    private VKmerList pathList; //use for BFSTravese
    private EdgeTypeList edgeTypeList; //use for BFSTravese
    private VKmer targetVertexId; //use for BFSTravese
    private READHEAD_ORIENTATION srcReadHeadOrientation; //use for BFSTravese
    private READHEAD_ORIENTATION destReadHeadOrientation; //use for BFSTravese
    private int totalBFSLength;
    private HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap;

    public BFSTraverseMessage() {
        super();
        pathList = new VKmerList();
        edgeTypeList = new EdgeTypeList();
        targetVertexId = new VKmer();
        readId = 0;
        srcReadHeadOrientation = READHEAD_ORIENTATION.UNFLIPPED;
        destReadHeadOrientation = READHEAD_ORIENTATION.UNFLIPPED;
        totalBFSLength = 0;
        scaffoldingMap = new HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>>();
    }

    public void reset() {
        super.reset();
        pathList.reset();
        edgeTypeList.clear();
        targetVertexId.reset(0);
        readId = 0;
        srcReadHeadOrientation = READHEAD_ORIENTATION.UNFLIPPED;
        destReadHeadOrientation = READHEAD_ORIENTATION.UNFLIPPED;
        totalBFSLength = 0;
        scaffoldingMap.clear();
    }

    public VKmerList getPathList() {
        return pathList;
    }

    public void setPathList(VKmerList pathList) {
        validMessageFlag |= VALID_MESSAGE.PATH_LIST_AND_EDGETYPE_LIST;
        this.pathList = pathList;
    }

    public EdgeTypeList getEdgeTypeList() {
        return edgeTypeList;
    }

    public void setEdgeTypeList(EdgeTypeList edgeDirsList) {
        validMessageFlag |= VALID_MESSAGE.PATH_LIST_AND_EDGETYPE_LIST;
        this.edgeTypeList.clear();
        this.edgeTypeList.addAll(edgeDirsList);
    }

    public VKmer getTargetVertexId() {
        return targetVertexId;
    }

    public void setTargetVertexId(VKmer targetVertexId) {
        validMessageFlag |= VALID_MESSAGE.TARGET_VERTEX_ID;
        this.targetVertexId.setAsCopy(targetVertexId);
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
        validMessageFlag |= VALID_MESSAGE.SRC_AND_DEST_READ_HEAD_ORIENTATION;
        this.srcReadHeadOrientation = srcReadHeadOrientation;
    }

    public READHEAD_ORIENTATION getDestReadHeadOrientation() {
        return destReadHeadOrientation;
    }

    public void setDestReadHeadOrientation(READHEAD_ORIENTATION destReadHeadOrientation) {
        validMessageFlag |= VALID_MESSAGE.SRC_AND_DEST_READ_HEAD_ORIENTATION;
        this.destReadHeadOrientation = destReadHeadOrientation;
    }

    public int getTotalBFSLength() {
        return totalBFSLength;
    }

    public void setTotalBFSLength(int totalBFSLength) {
        validMessageFlag |= VALID_MESSAGE.TOTAL_BFS_LENGTH;
        this.totalBFSLength = totalBFSLength;
    }
    
    public HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> getScaffoldingMap() {
        return scaffoldingMap;
    }

    public void setScaffoldingMap(HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap) {
        validMessageFlag |= VALID_MESSAGE.SCAFFOLDING_MAP;
        this.scaffoldingMap.clear();
        this.scaffoldingMap.putAll(scaffoldingMap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        readId = in.readLong();
        if ((validMessageFlag & VALID_MESSAGE.PATH_LIST_AND_EDGETYPE_LIST) > 0){
            pathList.readFields(in);
            edgeTypeList.readFields(in);
        }
        if ((validMessageFlag & VALID_MESSAGE.TARGET_VERTEX_ID) > 0)
            targetVertexId.readFields(in);
        if ((validMessageFlag & VALID_MESSAGE.SRC_AND_DEST_READ_HEAD_ORIENTATION) > 0){
            srcReadHeadOrientation = READHEAD_ORIENTATION.fromByte(in.readByte());
            destReadHeadOrientation = READHEAD_ORIENTATION.fromByte(in.readByte());
        }
        if ((validMessageFlag & VALID_MESSAGE.TOTAL_BFS_LENGTH) > 0)
            totalBFSLength = in.readInt();
        if ((validMessageFlag & VALID_MESSAGE.SCAFFOLDING_MAP) > 0)
            scaffoldingMap.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(readId);
        if ((validMessageFlag & VALID_MESSAGE.PATH_LIST_AND_EDGETYPE_LIST) > 0){
            pathList.write(out);
            edgeTypeList.write(out);
        }
        if ((validMessageFlag & VALID_MESSAGE.TARGET_VERTEX_ID) > 0)
            targetVertexId.write(out);
        if ((validMessageFlag & VALID_MESSAGE.SRC_AND_DEST_READ_HEAD_ORIENTATION) > 0){
            out.writeByte(srcReadHeadOrientation.get());
            out.writeByte(destReadHeadOrientation.get());
        }
        if ((validMessageFlag & VALID_MESSAGE.TOTAL_BFS_LENGTH) > 0)
            out.writeInt(totalBFSLength);
        if ((validMessageFlag & VALID_MESSAGE.SCAFFOLDING_MAP) > 0)
            scaffoldingMap.write(out);
    }
}
