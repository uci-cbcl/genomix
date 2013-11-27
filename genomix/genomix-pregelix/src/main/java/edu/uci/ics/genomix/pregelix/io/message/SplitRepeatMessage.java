package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;

public class SplitRepeatMessage extends MessageWritable {

    protected class SPLITREPEAT_MESSAGE_FIELDS extends MESSAGE_FIELDS {
        public static final byte CREATED_EDGE = 1 << 1; // used in subclass: SplitRepeatMessage
    }

    private Entry<VKmer, ReadIdSet> createdEdge;

    public SplitRepeatMessage() {
        super();
        createdEdge = null;
    }

    @Override
    public void reset() {
        super.reset();
        createdEdge = null;
    }

    public Entry<VKmer, ReadIdSet> getCreatedEdge() {
        if (createdEdge == null) {
            createdEdge = new SimpleEntry<VKmer, ReadIdSet>(new VKmer(), new ReadIdSet());
        }
        return createdEdge;
    }

    public void setCreatedEdge(VKmer createdKmer, ReadIdSet createdReadIds) {
        createdEdge = new SimpleEntry<VKmer, ReadIdSet>(createdKmer, createdReadIds);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if ((messageFields & SPLITREPEAT_MESSAGE_FIELDS.CREATED_EDGE) != 0) {
            VKmer createdKmer = new VKmer();
            createdKmer.readFields(in);
            ReadIdSet createdReadIds = new ReadIdSet();
            createdReadIds.readFields(in);
            setCreatedEdge(createdKmer, createdReadIds);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if (createdEdge != null) {
            createdEdge.getKey().write(out);
            createdEdge.getValue().write(out);
        }
    }

    @Override
    protected byte getActiveMessageFields() {
        byte messageFields = super.getActiveMessageFields();
        if (createdEdge != null) {
            messageFields |= SPLITREPEAT_MESSAGE_FIELDS.CREATED_EDGE;
        }
        return messageFields;
    }
}
