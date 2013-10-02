package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;

public class SplitRepeatMessageWritable extends MessageWritable {
    
    private Entry<VKmer, ReadIdSet> createdEdge;
    private Entry<VKmer, ReadIdSet> deletedEdge;
    
    public SplitRepeatMessageWritable(){
        super();
    }
    
    public Entry<VKmer, ReadIdSet> getCreatedEdge() {
        return createdEdge;
    }

    public void setCreatedEdge(VKmer createdKmer, ReadIdSet createdReadIds) {
        this.createdEdge = new SimpleEntry<VKmer, ReadIdSet>(createdKmer, createdReadIds);
    }

    public Entry<VKmer, ReadIdSet> getDeletedEdge() {
        return deletedEdge;
    }

    public void setDeletedEdge(VKmer deletedKmer, ReadIdSet deletedReadIds) {
        this.deletedEdge = new SimpleEntry<VKmer, ReadIdSet>(deletedKmer, deletedReadIds);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        
        VKmer createdKmer = new VKmer();
        createdKmer.readFields(in);
        ReadIdSet createdReadIds = new ReadIdSet();
        createdReadIds.readFields(in);
        setCreatedEdge(createdKmer, createdReadIds);
        
        VKmer deletedKmer = new VKmer();
        deletedKmer.readFields(in);
        ReadIdSet deletedReadIds = new ReadIdSet();
        deletedReadIds.readFields(in);
        setDeletedEdge(deletedKmer, deletedReadIds);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        createdEdge.getKey().write(out);
        createdEdge.getValue().write(out);
        deletedEdge.getKey().write(out);
        deletedEdge.getValue().write(out);
    }
}
