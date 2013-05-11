package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Position implements Writable {
    public int readID;
    public byte posInRead;

    public Position() {
        readID = 0;
        posInRead = 0;
    }

    public Position(int readID, byte posInRead) {
        this.readID = readID;
        this.posInRead = posInRead;
    }

    public Position(final Position pos) {
        this.readID = pos.readID;
        this.posInRead = pos.posInRead;
    }

    public void set(int readID, byte posInRead) {
        this.readID = readID;
        this.posInRead = posInRead;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readID = in.readInt();
        posInRead = in.readByte();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(readID);
        out.writeByte(posInRead);
    }

}
