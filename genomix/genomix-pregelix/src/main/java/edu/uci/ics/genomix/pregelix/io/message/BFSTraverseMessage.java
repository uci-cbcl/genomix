package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.EdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.vertex.SearchInfo;
import edu.uci.ics.genomix.type.Node.READHEAD_ORIENTATION;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

public class BFSTraverseMessage extends MessageWritable {

    protected class BFS_MESSAGE_FIELDS extends MESSAGE_FIELDS {
        public static final byte PATH_LIST_AND_EDGETYPE_LIST = 1 << 1; // used in BFSTraverseMessage
        public static final byte SRC_AND_DEST_READ_HEAD_ORIENTATION = 1 << 2; // used in BFSTraverseMessage
        public static final byte TARGET_VERTEX_ID = 1 << 3; // used in BFSTraverseMessage
        public static final byte READ_ID = 1 << 4;
        public static final byte TOTAL_BFS_LENGTH = 1 << 5; // used in BFSTraverseMessage
        public static final byte SCAFFOLDING_MAP = 1 << 6; // used in BFSTraverseMessage
    }

    private VKmerList pathList; //use for BFSTravese
    private EdgeTypeList edgeTypeList; //use for BFSTravese
    private VKmer targetVertexId; //use for BFSTravese
    private Long readId; //use for BFSTravese
    private READHEAD_ORIENTATION srcReadHeadOrientation; //use for BFSTravese
    private READHEAD_ORIENTATION destReadHeadOrientation; //use for BFSTravese
    private Integer totalBFSLength;
    private HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap;

    public BFSTraverseMessage() {
        super();
        pathList = null;
        edgeTypeList = null;
        targetVertexId = null;
        readId = null;
        srcReadHeadOrientation = null;
        destReadHeadOrientation = null;
        totalBFSLength = null;
        scaffoldingMap = null;
    }

    public void reset() {
        super.reset();
        pathList = null;
        edgeTypeList = null;
        targetVertexId = null;
        readId = null;
        srcReadHeadOrientation = null;
        destReadHeadOrientation = null;
        totalBFSLength = null;
        scaffoldingMap = null;
    }

    public VKmerList getPathList() {
        if (pathList == null) {
            pathList = new VKmerList();
        }
        return pathList;
    }

    public void setPathList(VKmerList pathList) {
        getPathList().setAsCopy(pathList); // TODO should be a copy?
    }

    public EdgeTypeList getEdgeTypeList() {
        if (edgeTypeList == null) {
            edgeTypeList = new EdgeTypeList();
        }
        return edgeTypeList;
    }

    public void setEdgeTypeList(EdgeTypeList edgeDirsList) {
        getEdgeTypeList().clear();
        getEdgeTypeList().addAll(edgeDirsList);
    }

    public VKmer getTargetVertexId() {
        if (targetVertexId == null) {
            targetVertexId = new VKmer();
        }
        return targetVertexId;
    }

    public void setTargetVertexId(VKmer targetVertexId) {
        getTargetVertexId().setAsCopy(targetVertexId);
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

    public int getTotalBFSLength() {
        return totalBFSLength;
    }

    public void setTotalBFSLength(int totalBFSLength) {
        this.totalBFSLength = totalBFSLength;
    }

    public HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> getScaffoldingMap() {
        if (scaffoldingMap == null) {
            scaffoldingMap = new HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>>();
        }
        return scaffoldingMap;
    }

    public void setScaffoldingMap(HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap) {
        getScaffoldingMap().clear();
        getScaffoldingMap().putAll(scaffoldingMap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if ((messageFields & BFS_MESSAGE_FIELDS.READ_ID) != 0) {
            readId = in.readLong();
        }
        if ((messageFields & BFS_MESSAGE_FIELDS.PATH_LIST_AND_EDGETYPE_LIST) != 0) {
            getPathList().readFields(in);
            getEdgeTypeList().readFields(in);
        }
        if ((messageFields & BFS_MESSAGE_FIELDS.TARGET_VERTEX_ID) != 0) {
            getTargetVertexId().readFields(in);
        }
        if ((messageFields & BFS_MESSAGE_FIELDS.SRC_AND_DEST_READ_HEAD_ORIENTATION) != 0) {
            srcReadHeadOrientation = READHEAD_ORIENTATION.fromByte(in.readByte());
            destReadHeadOrientation = READHEAD_ORIENTATION.fromByte(in.readByte());
        }
        if ((messageFields & BFS_MESSAGE_FIELDS.TOTAL_BFS_LENGTH) != 0) {
            totalBFSLength = in.readInt();
        }
        if ((messageFields & BFS_MESSAGE_FIELDS.SCAFFOLDING_MAP) != 0) {
            getScaffoldingMap().readFields(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if (readId != null) {
            out.writeLong(readId);
        }
        if (pathList != null || edgeTypeList != null) {
            pathList.write(out);
            edgeTypeList.write(out);
        }
        if (targetVertexId != null) {
            targetVertexId.write(out);
        }
        if (srcReadHeadOrientation != null || destReadHeadOrientation != null) {
            out.writeByte(srcReadHeadOrientation.get());
            out.writeByte(destReadHeadOrientation.get());
        }
        if (totalBFSLength != null) {
            out.writeInt(totalBFSLength);
        }
        if (scaffoldingMap != null) {
            scaffoldingMap.write(out);
        }
    }

    @Override
    protected byte getActiveMessageFields() {
        byte messageFields = super.getActiveMessageFields();
        if (readId != null) {
            messageFields |= BFS_MESSAGE_FIELDS.READ_ID;
        }
        if (pathList != null || edgeTypeList != null) {
            messageFields |= BFS_MESSAGE_FIELDS.PATH_LIST_AND_EDGETYPE_LIST;
        }
        if (targetVertexId != null) {
            messageFields |= BFS_MESSAGE_FIELDS.TARGET_VERTEX_ID;
        }
        if (srcReadHeadOrientation != null || destReadHeadOrientation != null) {
            messageFields |= BFS_MESSAGE_FIELDS.SRC_AND_DEST_READ_HEAD_ORIENTATION;
        }
        if (totalBFSLength != null) {
            messageFields |= BFS_MESSAGE_FIELDS.TOTAL_BFS_LENGTH;
        }
        if (scaffoldingMap != null) {
            messageFields |= BFS_MESSAGE_FIELDS.SCAFFOLDING_MAP;
        }
        return messageFields;
    }
}
