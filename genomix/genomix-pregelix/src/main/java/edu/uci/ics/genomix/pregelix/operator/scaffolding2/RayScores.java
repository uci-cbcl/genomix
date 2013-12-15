package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;

public class RayScores implements Writable {

    private HashMap<SimpleEntry<EDGETYPE, VKmer>, Rules> scores;

    // TODO make this an aggregator function which for an existing id, adds ruleA and ruleB but keeps the min of all ruleC's
    public void addScore(EDGETYPE et, VKmer kmer, int ruleA, int ruleB, int ruleC) {
        SimpleEntry<EDGETYPE, VKmer> key = new SimpleEntry<>(et, kmer);
        if (scores.containsKey(key)) {
            Rules r = scores.get(key);
            r.ruleAs += ruleA;
            r.ruleBs += ruleB;
            r.ruleCs = Math.min(r.ruleCs, ruleC);
        } else {
            scores.put(key, new Rules(ruleA, ruleB, ruleC));
        }
    }

    public void addAllScores(RayScores otherScores) {
        for (Entry<SimpleEntry<EDGETYPE, VKmer>, Rules> elem : scores.entrySet()) {
            addScore(elem.getKey().getKey(), elem.getKey().getValue(), elem.getValue().ruleAs, elem.getValue().ruleBs,
                    elem.getValue().ruleCs);
        }
    }

    /**
     * Return true iff queryKmer is "better than" targetKmer according to my scores.
     * Specifically, each of the rule values must be "factor" times larger than the corresponding values for targetKmer
     */
    public boolean dominates(EDGETYPE queryET, VKmer queryKmer, EDGETYPE targetET, VKmer targetKmer, float factor) {
        SimpleEntry<EDGETYPE, VKmer> queryKey = new SimpleEntry<>(queryET, queryKmer);
        SimpleEntry<EDGETYPE, VKmer> targetKey = new SimpleEntry<>(targetET, targetKmer);
        if (!scores.containsKey(queryKey)) {
            throw new IllegalArgumentException("Didn't find query in the score list! " + queryET + ":" + queryKmer);
        } else if (!scores.containsKey(targetKey)) {
            throw new IllegalArgumentException("Didn't find query in the score list! " + targetET + ":" + targetKmer);
        }
        Rules queryRule = scores.get(queryKey);
        Rules targetRule = scores.get(targetKey);
        return (((queryRule.ruleAs) > targetRule.ruleAs * factor)// 
                && ((queryRule.ruleBs) > targetRule.ruleBs * factor)// 
        && ((queryRule.ruleCs) > targetRule.ruleCs * factor));//
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            EDGETYPE et = EDGETYPE.fromByte(in.readByte());
            VKmer kmer = new VKmer();
            kmer.readFields(in);
            Rules rules = new Rules();
            rules.readFields(in);
            scores.put(new SimpleEntry<>(et, kmer), rules);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(scores.size());
        for (Entry<SimpleEntry<EDGETYPE, VKmer>, Rules> elem : scores.entrySet()) {
            out.writeByte(elem.getKey().getKey().ordinal());
            elem.getKey().getValue().write(out);
            elem.getValue().write(out);
        }
    }

    public void clear() {
        scores.clear();
    }

    public int size() {
        return scores.size();
    }

    public void setAsCopy(RayScores otherScores) {
        clear();
        addAllScores(otherScores);
    }

    private class Rules {
        public int ruleAs;
        public int ruleBs;
        public int ruleCs;

        public Rules() {

        }

        public Rules(int ruleAs, int ruleBs, int ruleCs) {
            this.ruleAs = ruleAs;
            this.ruleBs = ruleBs;
            this.ruleCs = ruleCs;
        }

        public void readFields(DataInput in) throws IOException {
            ruleAs = in.readInt();
            ruleBs = in.readInt();
            ruleCs = in.readInt();
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(ruleAs);
            out.writeInt(ruleBs);
            out.writeInt(ruleCs);
        }
    }
}
