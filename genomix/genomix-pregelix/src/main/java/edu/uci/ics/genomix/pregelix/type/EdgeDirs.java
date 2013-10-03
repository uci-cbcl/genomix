package edu.uci.ics.genomix.pregelix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class EdgeDirs implements Writable {

    private byte prevToMeDir;
    private byte nextToMeDir;
    private boolean hasPrevDir;
    private boolean hasNextDir;

    public EdgeDirs() {
        prevToMeDir = 0;
        nextToMeDir = 0;
        hasPrevDir = false;
        hasNextDir = false;
    }

    public EdgeDirs(EdgeDirs other) {
        set(other);
    }

    public void set(EdgeDirs edgeDirs) {
        prevToMeDir = edgeDirs.prevToMeDir;
        nextToMeDir = edgeDirs.nextToMeDir;
        hasPrevDir = edgeDirs.hasPrevDir;
        hasNextDir = edgeDirs.hasNextDir;
    }

    public void reset() {
        prevToMeDir = 0;
        nextToMeDir = 0;
        hasPrevDir = false;
        hasNextDir = false;
    }

    public byte getPrevToMeDir() {
        return prevToMeDir;
    }

    public void setPrevToMeDir(byte prevToMeDir) {
        this.hasPrevDir = true;
        this.prevToMeDir = prevToMeDir;
    }

    public byte getNextToMeDir() {
        return nextToMeDir;
    }

    public void setNextToMeDir(byte nextToMeDir) {
        this.hasNextDir = true;
        this.nextToMeDir = nextToMeDir;
    }

    public boolean isHasPrevDir() {
        return hasPrevDir;
    }

    public void setHasPrevDir(boolean hasPrevDir) {
        this.hasPrevDir = hasPrevDir;
    }

    public boolean isHasNextDir() {
        return hasNextDir;
    }

    public void setHasNextDir(boolean hasNextDir) {
        this.hasNextDir = hasNextDir;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.prevToMeDir = in.readByte();
        this.nextToMeDir = in.readByte();
        this.hasPrevDir = in.readBoolean();
        this.hasNextDir = in.readBoolean();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(this.prevToMeDir);
        out.writeByte(this.nextToMeDir);
        out.writeBoolean(this.hasPrevDir);
        out.writeBoolean(this.hasNextDir);
    }

}
