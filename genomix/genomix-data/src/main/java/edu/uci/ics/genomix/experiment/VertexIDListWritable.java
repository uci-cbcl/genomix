package edu.uci.ics.genomix.experiment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class VertexIDListWritable implements Writable {

    private PositionList vertexIDList;
    private int[] tempReadIDList = null;
    private byte[] tempPosInReadList = null;
    
    public VertexIDListWritable() {
        this.vertexIDList = new PositionList();
    }
    
    public VertexIDListWritable(PositionList right) {
        this.vertexIDList = new PositionList(right);
    }

    public VertexIDListWritable(int length) {
        this.vertexIDList = new PositionList(length);
    }
    
    public void set(VertexIDListWritable right) {
        set(right.get());
    }
    public void set(PositionList right) {
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

//    Position [] pos  = new Position();
    @Override
    public void write(DataOutput out) throws IOException {
//        out.writeInt(length);
//        for ( int i: length){
//            pos[i].write(out);
//        }
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
    
    public PositionList get() {
        return vertexIDList;
    }
}
