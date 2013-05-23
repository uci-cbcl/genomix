package edu.uci.ics.genomix.hadoop.velvetgraphbuilding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;

public class PositionListAndKmerWritable implements WritableComparable<PositionListAndKmerWritable> {
    
    private PositionListWritable vertexIDList;
    private KmerBytesWritable kmer;
    private int countOfKmer;
    
    public PositionListAndKmerWritable(){
        countOfKmer = 0;
        vertexIDList = new PositionListWritable();
        kmer = new KmerBytesWritable();
    }

    public PositionListAndKmerWritable(int kmerSize) {
        countOfKmer = 0;
        vertexIDList = new PositionListWritable();
        kmer = new KmerBytesWritable(kmerSize);
    }

    public int getCount() {
        return countOfKmer;
    }

    public void setCount(int count) {
        this.countOfKmer = count;
    }

    public void setvertexIDList(PositionListWritable posList) {
        vertexIDList.set(posList);
    }

    public void reset() {
        vertexIDList.reset();
        countOfKmer = 0;
    }

    public PositionListWritable getVertexIDList() {
        return vertexIDList;
    }

    public KmerBytesWritable getKmer() {
        return kmer;
    }

    public void set(PositionListAndKmerWritable right) {
        this.countOfKmer = right.countOfKmer;
        this.vertexIDList.set(right.vertexIDList);
        this.kmer.set(right.kmer);
    }

    public void set(PositionListWritable list, KmerBytesWritable kmer) {
        this.vertexIDList.set(list);
        this.kmer.set(kmer);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.countOfKmer = in.readInt();
        this.vertexIDList.readFields(in);
        this.kmer.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.countOfKmer);
        this.vertexIDList.write(out);
        this.kmer.write(out);
    }

    @Override
    public int compareTo(PositionListAndKmerWritable o) {
        return 0;
    }
}
