package edu.uci.ics.genomix.pregelix.io.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.VKmerListWritable;

public class BFSTraverseMessageWritable extends MessageWritable{
    
    private VKmerListWritable pathList; //use for BFSTravese
    private boolean srcFlip; //use for BFSTravese
    private boolean destFlip; //use for BFSTravese
    
    public BFSTraverseMessageWritable(){
        super();
        pathList = new VKmerListWritable();
        srcFlip = false;
        destFlip = false;
    }
    
    public void reset(){
        super.reset();
        pathList.reset();
        srcFlip = false;
        destFlip = false;
    }

    public VKmerListWritable getPathList() {
        return pathList;
    }

    public void setPathList(VKmerListWritable pathList) {
        this.pathList = pathList;
    }

    public boolean isSrcFlip() {
        return srcFlip;
    }

    public void setSrcFlip(boolean srcFlip) {
        this.srcFlip = srcFlip;
    }

    public boolean isDestFlip() {
        return destFlip;
    }

    public void setDestFlip(boolean destFlip) {
        this.destFlip = destFlip;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        pathList.readFields(in);
        srcFlip = in.readBoolean();
        destFlip = in.readBoolean();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        pathList.write(out);
        out.writeBoolean(srcFlip);
        out.writeBoolean(destFlip);
    }
}
