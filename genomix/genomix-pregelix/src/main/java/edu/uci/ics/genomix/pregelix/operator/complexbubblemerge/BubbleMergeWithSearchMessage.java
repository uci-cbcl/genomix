package edu.uci.ics.genomix.pregelix.operator.complexbubblemerge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.complexbubblemerge.BubbleMergeWithSearchVertexValueWritable.EdgeTypeList;

public class BubbleMergeWithSearchMessage extends MessageWritable {

    protected class BUBBLEMERGE_WITH_SEARCH_FIELDS extends MESSAGE_FIELDS {
        public static final byte PRE_KMER_LENGTH = 1 << 1;
        public static final byte INTERNAL_KMER = 1 << 2;
        public static final byte PATH_LIST = 1 << 3;
        public static final byte EDGETYPE_LIST = 1 << 4;
        public static final byte NUM_BRANCHES = 1 << 5;
    }

    private Integer preKmerLength;
    private VKmer internalKmer;
    private VKmerList pathList;
    private EdgeTypeList edgeTypeList;
    private Integer numBranches;

    public BubbleMergeWithSearchMessage() {
        super();
        preKmerLength = null;
        internalKmer = null;
        pathList = null;
        numBranches = null;
        edgeTypeList = null;
    }

    @Override
    public void reset() {
        super.reset();
        preKmerLength = null;
        internalKmer = null;
        pathList = null;
        numBranches = null;
        edgeTypeList = null;
    }

    public Integer getPreKmerLength() {
        return preKmerLength;
    }

    public void setPreKmerLength(int preKmerLength) {
        this.preKmerLength = preKmerLength;
    }

    public VKmer getInternalKmer() {
        if (internalKmer == null)
            internalKmer = new VKmer();
        return internalKmer;
    }

    public void setInternalKmer(VKmer internalKmer) {
        getInternalKmer().setAsCopy(internalKmer);
    }

    public VKmerList getPathList() {
        if (pathList == null)
            pathList = new VKmerList();
        return pathList;
    }

    public void setPathList(VKmerList pathList) {
        getPathList().setAsCopy(pathList);
    }

    public EdgeTypeList getEdgeTypeList() {
        if (edgeTypeList == null)
            edgeTypeList = new EdgeTypeList();
        return edgeTypeList;
    }

    public void setEdgeTypeList(EdgeTypeList edgeTypeList) {
        if (edgeTypeList != null) {
            getEdgeTypeList().clear();
            for (EDGETYPE et : edgeTypeList) {
                getEdgeTypeList().add(et);
            }
        }
    }

    public Integer getNumBranches() {
        return numBranches;
    }

    public void setNumBranches(int numBranches) {
        this.numBranches = numBranches;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if ((messageFields & BUBBLEMERGE_WITH_SEARCH_FIELDS.PRE_KMER_LENGTH) != 0) {
            setPreKmerLength(in.readInt());
        }
        if ((messageFields & BUBBLEMERGE_WITH_SEARCH_FIELDS.INTERNAL_KMER) != 0) {
            getInternalKmer().readFields(in);
        }
        if ((messageFields & BUBBLEMERGE_WITH_SEARCH_FIELDS.PATH_LIST) != 0) {
            getPathList().readFields(in);
        }
        if ((messageFields & BUBBLEMERGE_WITH_SEARCH_FIELDS.EDGETYPE_LIST) != 0) {
            getEdgeTypeList().readFields(in);
        }
        if ((messageFields & BUBBLEMERGE_WITH_SEARCH_FIELDS.NUM_BRANCHES) != 0) {
            setNumBranches(in.readInt());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if (preKmerLength != null) {
            out.writeInt(preKmerLength);
        }
        if (internalKmer != null && internalKmer.getKmerLetterLength() > 0) {
            internalKmer.write(out);
        }
        if (pathList != null && pathList.size() > 0) {
            pathList.write(out);
        }
        if (edgeTypeList != null && edgeTypeList.size() > 0) {
            edgeTypeList.write(out);
        }
        if (numBranches != null) {
            out.writeInt(numBranches);
        }
    }

    @Override
    protected byte getActiveMessageFields() {
        byte messageFields = super.getActiveMessageFields();
        if (preKmerLength != null) {
            messageFields |= BUBBLEMERGE_WITH_SEARCH_FIELDS.PRE_KMER_LENGTH;
        }
        if (internalKmer != null && internalKmer.getKmerLetterLength() > 0) {
            messageFields |= BUBBLEMERGE_WITH_SEARCH_FIELDS.INTERNAL_KMER;
        }
        if (pathList != null && pathList.size() > 0) {
            messageFields |= BUBBLEMERGE_WITH_SEARCH_FIELDS.PATH_LIST;
        }
        if (edgeTypeList != null && edgeTypeList.size() > 0) {
            messageFields |= BUBBLEMERGE_WITH_SEARCH_FIELDS.EDGETYPE_LIST;
        }
        if (numBranches != null) {
            messageFields |= BUBBLEMERGE_WITH_SEARCH_FIELDS.NUM_BRANCHES;
        }
        return messageFields;
    }
}
