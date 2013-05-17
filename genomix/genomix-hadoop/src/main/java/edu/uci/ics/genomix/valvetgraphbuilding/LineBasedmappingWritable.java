package edu.uci.ics.genomix.valvetgraphbuilding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import edu.uci.ics.genomix.type.PositionListWritable;

public class LineBasedmappingWritable extends PositionListWritable{
    byte posInRead;
    
    public LineBasedmappingWritable() {
        super();
        this.posInRead = -1;        
    }

    public LineBasedmappingWritable(int count, byte [] data, int offset, byte posInRead) {       
        super(count, data, offset);
        this.posInRead = posInRead;
    }
    
    public void set(byte posInRead, PositionListWritable right) {
        super.set(right);
        this.posInRead = posInRead;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.posInRead = in.readByte();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeByte(this.posInRead);
    }
    
    public int getPosInInvertedIndex() {
        return this.posInRead;
    }
}
