package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.type.VKmerListWritable;

public class AdjacencyListWritable implements WritableComparable<AdjacencyListWritable>{
    private VKmerListWritable forwardList;
    private VKmerListWritable reverseList;
    
    public AdjacencyListWritable(){
        forwardList = new VKmerListWritable();
        reverseList = new VKmerListWritable();
    }
    
    public AdjacencyListWritable(int kmerSize){
        forwardList = new VKmerListWritable();
        reverseList = new VKmerListWritable();
    }

    public void set(AdjacencyListWritable adjacencyList){
        forwardList.setCopy(adjacencyList.getForwardList());
        reverseList.setCopy(adjacencyList.getReverseList());
    }
    
    public void reset(){
        forwardList.reset();
        reverseList.reset();
    }
    
    public void reset(int kmerSize){
        forwardList.reset();
        reverseList.reset();
    }
    
    public int getCountOfPosition(){
    	return forwardList.getCountOfPosition() + reverseList.getCountOfPosition();
    }

    public VKmerListWritable getForwardList() {
        return forwardList;
    }

    public void setForwardList(VKmerListWritable forwardList) {
        this.forwardList = forwardList;
    }

    public VKmerListWritable getReverseList() {
        return reverseList;
    }

    public void setReverseList(VKmerListWritable reverseList) {
        this.reverseList = reverseList;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        forwardList.readFields(in);
        reverseList.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        forwardList.write(out);
        reverseList.write(out);
    }

    @Override
    public int compareTo(AdjacencyListWritable o) {
        return 0;
    }
}
