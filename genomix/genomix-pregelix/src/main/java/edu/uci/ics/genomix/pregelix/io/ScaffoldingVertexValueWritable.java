package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;

public class ScaffoldingVertexValueWritable extends VertexValueWritable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private HashMapWritable<LongWritable, PathAndEdgeTypeList> pathMap;
    private HashMapWritable<LongWritable, BooleanWritable> unambiguousReadIds;
    
    public ScaffoldingVertexValueWritable(){
        super();
        pathMap = new HashMapWritable<LongWritable, PathAndEdgeTypeList>();
        unambiguousReadIds = new HashMapWritable<LongWritable, BooleanWritable>();
    }
    
    public void setAsCopy(ScaffoldingVertexValueWritable other) {
        super.setAsCopy(other);
        pathMap.clear();
        pathMap.putAll(other.pathMap);
        unambiguousReadIds.clear();
        unambiguousReadIds.putAll(other.getUnambiguousReadIds());
    }
    
    public HashMapWritable<LongWritable, PathAndEdgeTypeList> getPathMap() {
        return pathMap;
    }

    public void setPathMap(HashMapWritable<LongWritable, PathAndEdgeTypeList> pathMap) {
        this.pathMap.clear();
        this.pathMap.putAll(pathMap);
    }
    
    public HashMapWritable<LongWritable, BooleanWritable> getUnambiguousReadIds() {
        return unambiguousReadIds;
    }

    public void setUnambiguousReadIds(HashMapWritable<LongWritable, BooleanWritable> unambiguousReadIds) {
        this.unambiguousReadIds.clear();
        this.unambiguousReadIds.putAll(unambiguousReadIds);
    }

    public void reset() {
        super.reset();
        pathMap.clear();
        unambiguousReadIds.clear();
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        pathMap.readFields(in);
        unambiguousReadIds.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        pathMap.write(out);
        unambiguousReadIds.write(out);
    }
}
