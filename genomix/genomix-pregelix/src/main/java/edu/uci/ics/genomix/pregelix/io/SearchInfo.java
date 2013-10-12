package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.Node.READHEAD_ORIENTATION;

/**
 * SearchInfo stores information about destinationKmer and ifFlip(true == from EndReads; false == from startReads)
 * Used by scaffolding
 */
public class SearchInfo implements Writable {
    private VKmer srcOrDestKmer;
    private READHEAD_ORIENTATION readHeadOrientation;

    public SearchInfo() {
        srcOrDestKmer = new VKmer();
        this.readHeadOrientation = READHEAD_ORIENTATION.UNFLIPPED;
    }

    public SearchInfo(VKmer otherKmer, READHEAD_ORIENTATION readHeadOrientation) {
        this();
        this.srcOrDestKmer.setAsCopy(otherKmer);
        this.readHeadOrientation = readHeadOrientation;
    }

    public VKmer getSrcOrDestKmer() {
        return srcOrDestKmer;
    }

    public void setSrcOrDestKmer(VKmer srcOrDestKmer) {
        this.srcOrDestKmer = srcOrDestKmer;
    }

    public READHEAD_ORIENTATION getReadHeadOrientation() {
        return readHeadOrientation;
    }

    public void setReadHeadOrientation(READHEAD_ORIENTATION readHeadOrientation) {
        this.readHeadOrientation = readHeadOrientation;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        srcOrDestKmer.write(out);
        out.writeByte(readHeadOrientation.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        srcOrDestKmer.readFields(in);
        readHeadOrientation = READHEAD_ORIENTATION.fromByte(in.readByte());
    }

    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('(');
        sbuilder.append("SrcOrDestKmer: " + this.srcOrDestKmer + ", ");
        sbuilder.append("ReadHeadOrientation: " + this.readHeadOrientation);
        sbuilder.append(')');
        return sbuilder.toString();
    }
}

