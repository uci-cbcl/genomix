package edu.uci.ics.genomix.experiment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class VertexAdjacentWritable implements Writable {

    private static final byte[] EMPTY_BYTES = {};
    private VertexIDListWritable adjVertexList;
    private VKmerBytesWritable kmer;

    public VertexAdjacentWritable() {
        this(null, 0, EMPTY_BYTES);
    }

    public VertexAdjacentWritable(VertexIDList right, int kmerSize, byte[] bytes) {
        if (right != null)
            this.adjVertexList = new VertexIDListWritable(right);
        else
            this.adjVertexList = new VertexIDListWritable();
        this.kmer = new VKmerBytesWritable(kmerSize, bytes);
        kmer.set(bytes, 0, bytes.length);
    }

    public void set(VertexAdjacentWritable right) {
        set(right.getAdjVertexList(), right.getVkmer());
    }

    public void set(VertexIDListWritable vIDList, KmerBytesWritable kmer) {
        this.adjVertexList.set(vIDList);
        this.kmer.set(kmer);
    }
    
    public void set(VertexIDListWritable vIDList, VKmerBytesWritable kmer) {
        this.adjVertexList.set(vIDList);
        this.kmer.set(kmer);
    }
    
    @Override
    public void readFields(DataInput arg0) throws IOException {
        // TODO Auto-generated method stub
        adjVertexList.readFields(arg0);
        kmer.readFields(arg0);
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        // TODO Auto-generated method stub
        adjVertexList.write(arg0);
        kmer.write(arg0);
    }
    
    public VertexIDListWritable getAdjVertexList() {
        return this.adjVertexList;
    }
    
    public VKmerBytesWritable getVkmer() {
        return this.kmer;
    }
}
