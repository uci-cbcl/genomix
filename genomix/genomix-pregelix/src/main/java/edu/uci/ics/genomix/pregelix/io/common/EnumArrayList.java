package edu.uci.ics.genomix.pregelix.io.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class EnumArrayList<E extends ByteBacked<E>> extends ArrayList<E> implements Writable{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.clear();

        int numFields = in.readInt();
        if(numFields==0) return;
        
        for (int i = 0; i < numFields; i++) {
//            E e = 
        }
        
    }

}
