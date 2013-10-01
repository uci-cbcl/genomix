package edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class MsgListWritable implements WritableComparable<MsgListWritable>, Serializable,
        Iterable<MsgWritable> {

    /**
     * 
     */
    
    private static final long serialVersionUID = 1L;

    protected MsgWritable messageIter = new MsgWritable();

    protected ArrayList<MsgWritable> msgs;

    public MsgListWritable() {
        msgs = new ArrayList<MsgWritable>(1);
    }

    public MsgListWritable(MsgListWritable other) {
        this();
        setAsCopy(other);
    }

    public MsgListWritable(List<MsgWritable> otherList) {
        this();
        for (MsgWritable e : otherList) {
            add(e);
        }
    }

    public void setAsCopy(MsgListWritable otherMessage) {
        reset();
        for (MsgWritable o : otherMessage.msgs) {
            msgs.add(new MsgWritable(o));
        }
    }

    public void reset() {
        msgs.clear();
    }

    public MsgWritable get(int i) {
        return msgs.get(i);
    }

    public boolean add(MsgWritable element) {
        return msgs.add(new MsgWritable(element));
    }

    public MsgWritable set(int i, MsgWritable element) {
        return msgs.set(i, element);
    }

    public boolean isEmpty() {
        return getCountOfPosition() == 0;
    }

    public int getCountOfPosition() {
        return msgs.size();
    }

    public MsgWritable getMessage(VKmerBytesWritable key) {
        for (MsgWritable mes : msgs) {
            if (mes.getSourceVertexId().equals(key)) {
                return mes;
            }
        }
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(msgs.size());
        for (MsgWritable e : msgs) {
            e.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            MsgWritable e = new MsgWritable();
            e.readFields(in);
            msgs.add(e);
        }
    }

    @Override
    public int compareTo(MsgListWritable o) {
        int result = Integer.compare(msgs.size(), o.msgs.size()); 
        if (result != 0) {
            return result;
        }
        for (int i=0; i < msgs.size(); i++) {
            result = msgs.get(i).compareTo(o.msgs.get(i));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    @Override
    public int hashCode() {
        return msgs.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (! (o instanceof MsgListWritable))
            return false;
            
        MsgListWritable ew = (MsgListWritable) o;
        return compareTo(ew) == 0;
    }
    
    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        String delim = "";
        Object[] objs = msgs.toArray();
        Arrays.sort(objs);
        for (Object e : objs) {
            sbuilder.append(delim).append(e.toString());
            delim = ",";
        }
        sbuilder.append(']');
        return sbuilder.toString();
    }
    
    @Override
    public Iterator<MsgWritable> iterator() {
        return msgs.iterator();
    }
    
    public Iterator<VKmerBytesWritable> getKeys() {
        Iterator<VKmerBytesWritable> it = new Iterator<VKmerBytesWritable>() {

            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return currentIndex < msgs.size();
            }

            @Override
            public VKmerBytesWritable next() {
                return msgs.get(currentIndex++).getSourceVertexId();
            }

            @Override
            public void remove() {
                msgs.remove(--currentIndex);
            }
        };
        return it;
    }
    
    public boolean contains(VKmerBytesWritable toFind){
        Iterator<VKmerBytesWritable> posIterator = this.getKeys();
        while (posIterator.hasNext()) {
            if (toFind.equals(posIterator.next()))
                return true;
        }
        return false;
    }
    
}
