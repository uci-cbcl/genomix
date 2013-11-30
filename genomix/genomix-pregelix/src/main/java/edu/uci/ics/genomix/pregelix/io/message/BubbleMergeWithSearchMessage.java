package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

public class BubbleMergeWithSearchMessage extends MessageWritable {

    protected class BUBBLEMERGE_WITH_SEARCH_FIELDS extends MESSAGE_FIELDS {
        public static final byte PRE_KMER_LENGTH = 1 << 1;
        public static final byte INTERNAL_KMER = 1 << 2;
        public static final byte PATH_LIST = 1 << 3;
        public static final byte NUM_BRANCHES = 1 << 4;
    }

    private Long preKmerLength;
    private VKmer internalKmer;
    private VKmerList pathList;
    private Integer numBranches;

    public BubbleMergeWithSearchMessage() {
        super();
        preKmerLength = null;
        internalKmer = null;
        pathList = null;
        numBranches = null;
    }

    @Override
    public void reset() {
        super.reset();
        preKmerLength = null;
        internalKmer = null;
        pathList = null;
        numBranches = null;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if ((messageFields & BUBBLEMERGE_WITH_SEARCH_FIELDS.PRE_KMER_LENGTH) != 0) {
            preKmerLength = in.readLong();
        }
        if ((messageFields & BUBBLEMERGE_WITH_SEARCH_FIELDS.INTERNAL_KMER) != 0) {
            internalKmer.readFields(in);
        }
        if ((messageFields & BUBBLEMERGE_WITH_SEARCH_FIELDS.PATH_LIST) != 0) {
            pathList.readFields(in);
        }
        if ((messageFields & BUBBLEMERGE_WITH_SEARCH_FIELDS.NUM_BRANCHES) != 0) {
            numBranches = in.readInt();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if (preKmerLength != null) {
            out.writeLong(preKmerLength);
        }
        if (internalKmer != null) {
            internalKmer.write(out);
        }
        if (pathList != null) {
            pathList.write(out);
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
        if (internalKmer != null) {
            messageFields |= BUBBLEMERGE_WITH_SEARCH_FIELDS.INTERNAL_KMER;
        }
        if (pathList != null) {
            messageFields |= BUBBLEMERGE_WITH_SEARCH_FIELDS.PATH_LIST;
        }
        if (numBranches != null) {
            messageFields |= BUBBLEMERGE_WITH_SEARCH_FIELDS.NUM_BRANCHES;
        }
        return messageFields;
    }
}
