package edu.uci.ics.genomix.experiment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class VertexIDListWritable implements Writable {

    private VertexIDList vertexIDList;
    private int[] tempReadIDList = null;
    private byte[] tempPosInReadList = null;
    
    public VertexIDListWritable() {
        this.vertexIDList = new VertexIDList();
    }
    
    public VertexIDListWritable(VertexIDList right) {
        this.vertexIDList = new VertexIDList(right);
    }

    public VertexIDListWritable(int length) {
        this.vertexIDList = new VertexIDList(length);
    }
    
    public void set(VertexIDListWritable right) {
        set(right.get());
    }
    public void set(VertexIDList right) {
        this.vertexIDList.set(right);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        int arraySize = in.readInt();
        int usedSize = in.readInt();
        if(usedSize > 0) {
            this.tempReadIDList = new int[arraySize];
            this.tempPosInReadList = new byte[arraySize];
            for(int i = 0; i < arraySize; i++){
                this.tempReadIDList[i] = in.readInt();
            }
            in.readFully(this.tempPosInReadList, 0, arraySize);
            this.vertexIDList.set(this.tempReadIDList, 0, this.tempPosInReadList, 0, usedSize);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeInt(vertexIDList.getArraySize());
        out.writeInt(vertexIDList.getUsedSize());
        if(vertexIDList.getUsedSize() > 0) {
            int[] readIDList = vertexIDList.getReadIDList();
            for(int i = 0 ; i < vertexIDList.getArraySize(); i ++ ){
                out.writeInt(readIDList[i]);
            }
            out.write(vertexIDList.getPosInReadList(), 0, vertexIDList.getArraySize());
        }
    }
    
    public VertexIDList get() {
        return vertexIDList;
    }
}
