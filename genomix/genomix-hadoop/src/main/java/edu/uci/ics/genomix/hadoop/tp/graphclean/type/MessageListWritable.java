package edu.uci.ics.genomix.hadoop.tp.graphclean.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.hadoop.tp.graphclean.io.MessageWritable;


public class MessageListWritable implements Writable{
    
    private ArrayList<MessageWritable> combineMsgList; 
    private MessageWritable temp;
    private Iterator<MessageWritable> it;
    private int length;
    
    public MessageListWritable() {
        combineMsgList = new ArrayList<MessageWritable>();
        temp = new MessageWritable();
    }
    
    public void setAsReference(ArrayList<MessageWritable> other, int sizeOfOther) {
        combineMsgList = (ArrayList<MessageWritable>) other.clone();
        length = sizeOfOther;
    }
    
    public ArrayList<MessageWritable> getContent() {
        return combineMsgList;
    }
    
    public int getLengthOfList() {
        return length;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.write(length);
        it = combineMsgList.iterator();
        while(it.hasNext()) {
            it.next().write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        length = in.readInt();
        for(int i = 0; i < length; i++) {
            temp.readFields(in);
            combineMsgList.add(new MessageWritable(temp));
        }
    }

}
