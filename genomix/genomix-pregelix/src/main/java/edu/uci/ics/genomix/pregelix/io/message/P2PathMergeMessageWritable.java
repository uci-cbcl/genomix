package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.NodeWritable;

public class P2PathMergeMessageWritable extends PathMergeMessageWritable{
    
    private NodeWritable prependMergeNode;
    private NodeWritable appendMergeNode;
    private byte prependMergeDir;
    private byte appendMergeDir;
    
    public P2PathMergeMessageWritable(){
        super();
        prependMergeNode = new NodeWritable();
        appendMergeNode = new NodeWritable();
        prependMergeDir = 0;
        appendMergeDir = 0;
    }

    public void reset(){
        super.reset();
        prependMergeNode.reset();
        appendMergeNode.reset();
        prependMergeDir = 0;
        appendMergeDir = 0;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        prependMergeNode.readFields(in);
        appendMergeNode.readFields(in);
        prependMergeDir = in.readByte();
        appendMergeDir = in.readByte();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        prependMergeNode.write(out);
        appendMergeNode.write(out);
        out.writeByte(prependMergeDir);
        out.writeByte(appendMergeDir);
    }
}
