package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;

public class SplitRepeatMessage extends MessageWritable {

    private Entry<VKmer, ReadIdSet> createdEdge;

    public SplitRepeatMessage() {
        super();
        createdEdge = new SimpleEntry<VKmer, ReadIdSet>(new VKmer(), new ReadIdSet());
    }

    public Entry<VKmer, ReadIdSet> getCreatedEdge() {
        return createdEdge;
    }

    public void setCreatedEdge(VKmer createdKmer, ReadIdSet createdReadIds) {
        validMessageFlag |= VALID_MESSAGE.CREATED_EDGE;
        this.createdEdge = new SimpleEntry<VKmer, ReadIdSet>(createdKmer, createdReadIds);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        if ((validMessageFlag & VALID_MESSAGE.CREATED_EDGE) > 0) {
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
        if ((validMessageFlag & VALID_MESSAGE.CREATED_EDGE) > 0) {
            createdEdge.getKey().write(out);
            createdEdge.getValue().write(out);
        }
    }
}
