package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.type.VKmerListWritable;

public class KmerListAndFlagListWritable implements Writable{
    private ArrayListWritable<BooleanWritable> flagList;
    private VKmerListWritable kmerList;
    
    public KmerListAndFlagListWritable(){
        flagList = new ArrayListWritable<BooleanWritable>();
        kmerList = new VKmerListWritable();
    }
    
    public void set(KmerListAndFlagListWritable kmerAndflag){
        flagList.clear();
        kmerList.reset();
        flagList.addAll(kmerAndflag.getFlagList());
        kmerList.appendList(kmerAndflag.getKmerList());
    }
    
    public void add(KmerListAndFlagListWritable otherKmerAndFlag){
        this.flagList.addAll(otherKmerAndFlag.getFlagList());
        this.kmerList.appendList(otherKmerAndFlag.getKmerList());
    }
    
    public int size(){
        return flagList.size();
    }
    
    public ArrayListWritable<BooleanWritable> getFlagList() {
        return flagList;
    }

    public void setFlagList(ArrayListWritable<BooleanWritable> flagList) {
        this.flagList.clear();
        this.flagList.addAll(flagList);
    }

    public VKmerListWritable getKmerList() {
        return kmerList;
    }

    public void setKmerList(VKmerListWritable kmerList) {
        this.kmerList.reset();
        this.kmerList.appendList(kmerList);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        flagList.write(out);
        kmerList.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        flagList.readFields(in);
        kmerList.readFields(in);
    }
    
}
