package edu.uci.ics.genomix.pregelix.io.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public class EntryWritable<K extends Writable, V extends Writable> 
    extends SimpleEntry<K, V> implements Writable{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public EntryWritable(K key, V value){
        super(key, value);
    }
    
    public EntryWritable(Entry<? extends K, ? extends V> entry) {
        super(entry);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        K key = getKey();
        V value = getValue();
        out.writeUTF(key.getClass().getCanonicalName());
        out.writeUTF(value.getClass().getCanonicalName());
        
        getKey().write(out);
        getValue().write(out);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void readFields(DataInput in) throws IOException {
        String keyClassName = in.readUTF();
        String valueClassName = in.readUTF();
        
        K key;
        V value;
        try {
            Class keyClass = Class.forName(keyClassName);
            Class valueClass = Class.forName(valueClassName);
            
            key = (K) keyClass.newInstance();
            key.readFields(in);
            value = (V) valueClass.newInstance();
            value.readFields(in);
            
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
    }

}
