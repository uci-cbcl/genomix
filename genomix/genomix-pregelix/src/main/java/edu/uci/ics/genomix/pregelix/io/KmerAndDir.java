package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.type.VKmer;

public class KmerAndDir implements Writable {

    public VKmer getDeleteKmer() {
        return deleteKmer;
    }

    public void setDeleteKmer(VKmer deleteKmer) {
        this.deleteKmer = deleteKmer;
    }

    public byte getDeleteDir() {
        return deleteDir;
    }

    public void setDeleteDir(byte deleteDir) {
        this.deleteDir = deleteDir;
    }

    private VKmer deleteKmer;
    private byte deleteDir;

    public KmerAndDir() {
        deleteKmer = new VKmer();
        deleteDir = 0;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        deleteKmer.readFields(in);
        deleteDir = in.readByte();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        deleteKmer.write(out);
        out.writeByte(deleteDir);
    }

}
