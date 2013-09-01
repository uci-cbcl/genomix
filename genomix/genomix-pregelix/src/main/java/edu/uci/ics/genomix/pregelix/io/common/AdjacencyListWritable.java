package edu.uci.ics.genomix.pregelix.io.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.type.EdgeListWritable;

public class AdjacencyListWritable implements WritableComparable<AdjacencyListWritable>{
    private EdgeListWritable forwardList;
    private EdgeListWritable reverseList;
    
    public AdjacencyListWritable(){
        forwardList = new EdgeListWritable();
        reverseList = new EdgeListWritable();
    }
    
    public AdjacencyListWritable(int kmerSize){
        forwardList = new EdgeListWritable();
        reverseList = new EdgeListWritable();
    }

    public void set(AdjacencyListWritable adjacencyList){
        forwardList.setAsCopy(adjacencyList.getForwardList());
        reverseList.setAsCopy(adjacencyList.getReverseList());
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

    public EdgeListWritable getForwardList() {
        return forwardList;
    }

    public void setForwardList(EdgeListWritable forwardList) {
        this.forwardList.setAsCopy(forwardList);
    }

    public EdgeListWritable getReverseList() {
        return reverseList;
    }

    public void setReverseList(EdgeListWritable reverseList) {
        this.reverseList.setAsCopy(reverseList);
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
