package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import edu.uci.ics.genomix.type.ReadIdListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class SplitRepeatMessageWritable extends MessageWritable {
    
    private Entry<VKmerBytesWritable, ReadIdListWritable> createdEdge;
    private Entry<VKmerBytesWritable, ReadIdListWritable> deletedEdge;
    
    public SplitRepeatMessageWritable(){
        super();
    }
    
    public Entry<VKmerBytesWritable, ReadIdListWritable> getCreatedEdge() {
        return createdEdge;
    }

    public void setCreatedEdge(VKmerBytesWritable createdKmer, ReadIdListWritable createdReadIds) {
        this.createdEdge = new SimpleEntry<VKmerBytesWritable, ReadIdListWritable>(createdKmer, createdReadIds);
    }

    public Entry<VKmerBytesWritable, ReadIdListWritable> getDeletedEdge() {
        return deletedEdge;
    }

    public void setDeletedEdge(VKmerBytesWritable deletedKmer, ReadIdListWritable deletedReadIds) {
        this.deletedEdge = new SimpleEntry<VKmerBytesWritable, ReadIdListWritable>(deletedKmer, deletedReadIds);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        
        VKmerBytesWritable createdKmer = new VKmerBytesWritable();
        createdKmer.readFields(in);
        ReadIdListWritable createdReadIds = new ReadIdListWritable();
        createdReadIds.readFields(in);
        setCreatedEdge(createdKmer, createdReadIds);
        
        VKmerBytesWritable deletedKmer = new VKmerBytesWritable();
        deletedKmer.readFields(in);
        ReadIdListWritable deletedReadIds = new ReadIdListWritable();
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
