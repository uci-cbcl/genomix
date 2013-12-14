package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;

public class RayScores implements Writable {
    private VKmerList scoredIds = new VKmerList();
    private ArrayList<Integer> ruleAs = new ArrayList<>();
    private ArrayList<Integer> ruleBs = new ArrayList<>();
    private ArrayList<Integer> ruleCs = new ArrayList<>();
    
    // TODO make this an aggregator function which for an existing id, adds ruleA and ruleB but keeps the min of all ruleC's
    public void addScore(VKmer id, int ruleA, int ruleB, int ruleC) {
        scoredIds.append(id);
        this.ruleAs.add(ruleA);
        this.ruleBs.add(ruleB);
        this.ruleCs.add(ruleC);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        scoredIds.readFields(in); 
        for (int i = 0; i < scoredIds.size(); i++) {
            ruleAs.add(in.readInt());
            ruleBs.add(in.readInt());
            ruleCs.add(in.readInt());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        scoredIds.write(out);
        for (int i = 0; i < scoredIds.size(); i++) {
            out.writeInt(ruleAs.get(i));
            out.writeInt(ruleBs.get(i));
            out.writeInt(ruleCs.get(i));
        }
    }
    
    public void reset() {
        scoredIds.clear();
        ruleAs.clear();
        ruleBs.clear();
        ruleCs.clear();
    }

    public int size() {
        return scoredIds.size();
    }

    public void setAsCopy(RayScores scores) {
        scoredIds.clear();
        for (VKmer kmer : scores.scoredIds) {
            scoredIds.append(new VKmer(kmer));
        }
        ruleAs.clear();
        ruleAs.addAll(scores.ruleAs);
        ruleBs.clear();
        ruleBs.addAll(scores.ruleBs);
        ruleCs.clear();
        ruleCs.addAll(scores.ruleCs);
    }

    public void aggregate(RayScores singleEndScores) {
        // TODO Auto-generated method stub
        
    }

    public boolean dominates(VKmer queryKmer, VKmer targetKmer) {
        // TODO Auto-generated method stub
        return false;
    }
}
