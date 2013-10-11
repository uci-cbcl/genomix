package edu.uci.ics.genomix.pregelix.io.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.EDGETYPE;

public class EdgeTypeList extends ArrayListWritable<EDGETYPE>{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void readFields(DataInput in) throws IOException {

        this.clear();

        int numFields = in.readInt();
        if(numFields==0) return;
        for (int i = 0; i < numFields; i++) {
            
        }
        
        
//        String className = in.readUTF();
//        E obj;
//        try {
//            Class c = Class.forName(className);
//            for (int i = 0; i < numFields; i++) {
//                obj = (E) c.newInstance();
//                obj.readFields(in);
//                this.add(obj);
//            }
//
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        } catch (IllegalAccessException e) {
//            e.printStackTrace();
//        } catch (InstantiationException e) {
//            e.printStackTrace();
//        }
    }
    
    public void write(DataOutput out) throws IOException {
//        out.writeInt(this.size());
//        if(size()==0) return;
//        E obj=get(0);
//        
//        out.writeUTF(obj.getClass().getCanonicalName());
//
//        for (int i = 0; i < size(); i++) {
//            obj = get(i);
//            if (obj == null) {
//                throw new IOException("Cannot serialize null fields!");
//            }
//            obj.write(out);
//        }
    }
}
