package edu.uci.ics.genomix.hadoop.experiments;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.experiment.VertexAdjacentWritable;
import edu.uci.ics.genomix.experiment.PositionList;
import edu.uci.ics.genomix.experiment.VertexIDListWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class LineBasedmappingWritable extends VertexAdjacentWritable{
    int posInInvertedIndex;
    
    public LineBasedmappingWritable() {
        super();
        this.posInInvertedIndex = -1;        
    }

    public LineBasedmappingWritable(int posInInvertedIndex, PositionList right, int kmerSize, byte[] bytes) {       
        super(right, kmerSize, bytes);
        this.posInInvertedIndex = posInInvertedIndex;
    }
    
    public void set(int posInInvertedIndex, VertexAdjacentWritable right) {
        super.set(right.getAdjVertexList(), right.getVkmer());
        this.posInInvertedIndex = posInInvertedIndex;
    }

    public void set(int posInInvertedIndex, VertexIDListWritable vIDList, VKmerBytesWritable kmer) {
        super.set(vIDList, kmer);
        this.posInInvertedIndex = posInInvertedIndex;
    }
    
    public void set(int posInInvertedIndex, VertexIDListWritable vIDList, KmerBytesWritable kmer) {
        super.set(vIDList, kmer);
        this.posInInvertedIndex = posInInvertedIndex;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.posInInvertedIndex = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(this.posInInvertedIndex);
    }
    
    public int getPosInInvertedIndex() {
        return this.posInInvertedIndex;
    }
}
