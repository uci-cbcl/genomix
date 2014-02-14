package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;

public class RayScores implements Writable {

    private HashMap<SimpleEntry<EDGETYPE, VKmer>, Rules> scores = new HashMap<>();
    
    public RayScores() {
    }
    
    public RayScores(RayScores other) {
        scores.clear();
        addAll(other);
    }

    private class Rules {
        public int ruleA = 0; // the overlap-weighted score (reads that overlap the walk better receive higher ruleA values)
        public int ruleB = 0; // the raw score
        public int ruleC = Integer.MAX_VALUE; // the smallest score seen in a single node

        public Rules() {
        }

        public Rules(int ruleAs, int ruleBs, int ruleCs) {
            this.ruleA = ruleAs;
            this.ruleB = ruleBs;
            this.ruleC = ruleCs;
        }

        public void readFields(DataInput in) throws IOException {
            ruleA = in.readInt();
            ruleB = in.readInt();
            ruleC = in.readInt();
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(ruleA);
            out.writeInt(ruleB);
            out.writeInt(ruleC);
        }

        @Override
        public String toString() {
            return "A:" + ruleA + " B:" + ruleB + " C:" + ruleC;
        }
    }

    public void addRuleCounts(EDGETYPE et, VKmer kmer, int ruleA, int ruleB, int ruleC) {
        SimpleEntry<EDGETYPE, VKmer> key = new SimpleEntry<>(et, kmer);
        if (scores.containsKey(key)) {
            Rules r = scores.get(key);
            r.ruleA += ruleA;
            r.ruleB += ruleB;
            r.ruleC = ruleC > 0 ? (r.ruleC > 0 ? Math.min(r.ruleC, ruleC) : ruleC) : r.ruleC; // min of non-zero values
        } else {
            scores.put(key, new Rules(ruleA, ruleB, ruleC));
        }
    }

    public void addAll(RayScores otherScores) {
        for (Entry<SimpleEntry<EDGETYPE, VKmer>, Rules> elem : otherScores.scores.entrySet()) {
            addRuleCounts(elem.getKey().getKey(), elem.getKey().getValue(), elem.getValue().ruleA,
                    elem.getValue().ruleB, elem.getValue().ruleC);
        }
    }
    
    public SimpleEntry<EDGETYPE, VKmer> getSingleKey(VKmer id) {
       // if (scores.size() != 1) {
       //     throw new IllegalStateException("requested single key but this score has " + scores.size() + " entries! " + scores);
       // }
       
        for (SimpleEntry<EDGETYPE,VKmer> e : scores.keySet()) {
        	if ((e.getValue().toString().substring(0, 20).equals(id.toString().substring(1, 21))) ||
        		(e.getValue().reverse().toString().substring(0, 20).equals(id.toString().substring(1, 21)))||
        		(e.getValue().toString().substring(0, 20).equals(id.reverse().toString().substring(1, 21)))||
        		(e.getValue().reverse().toString().substring(0, 20).equals(id.reverse().toString().substring(1, 21)))){
        		return e;
        	} else if(scores.size() == 1){
        		return null;
        	}
            
        }

        throw new IllegalStateException("requested single key but this score has " + scores.size() + " entries! " + scores);
    }
    
    
    public VKmer getVkmer(){
    	if (scores.size() != 1) {
    	    throw new IllegalStateException("requested single key but this score has " + scores.size() + " entries! " + scores);
    	}
    	
    	for (SimpleEntry<EDGETYPE,VKmer> e : scores.keySet()) {
    		return e.getValue();
    	}
    	throw new IllegalStateException("requested single kmer to add but this score has " + scores.size() + " entries! " + scores);
    }

    /**
     * Return true iff queryKmer is "better than" targetKmer according to my scores.
     * Specifically, each of the rule values must be "factor" times larger than the corresponding values for targetKmer
     */
    public boolean dominates(EDGETYPE queryET, VKmer queryKmer, EDGETYPE targetET, VKmer targetKmer,
            float frontierCoverage) {
        double factor = getMFactor(frontierCoverage);
        SimpleEntry<EDGETYPE, VKmer> queryKey = new SimpleEntry<>(queryET, queryKmer);
        SimpleEntry<EDGETYPE, VKmer> targetKey = new SimpleEntry<>(targetET, targetKmer);
        if (!scores.containsKey(queryKey)) {
            return false;
        } else if (!scores.containsKey(targetKey)) {
            scores.put(targetKey, new Rules()); // fill all rules with 0 // TODO what will 0 mean for ruleC?
        }
        Rules queryRule = scores.get(queryKey);
        Rules targetRule = scores.get(targetKey);
        return (((queryRule.ruleA) > targetRule.ruleA * factor) // overlap-weighted score  
                && ((queryRule.ruleB) > targetRule.ruleB * factor) // raw score 
//        && ((queryRule.ruleC) > targetRule.ruleC * factor)
        ); // smallest non-zero element of the walk
        // TODO ruleC again doesn't make much sense in our case-- the min is over nodes currently which may represent long kmers  
    }

    private double getMFactor(float frontierCoverage) {
        if (frontierCoverage >= 2 && frontierCoverage <= 19) {
            //return 3;
        	return 2.5;
        } else if (frontierCoverage >= 20 && frontierCoverage <= 24) {
            return 2;
            //return 1.1;
        } else if (frontierCoverage >= 25) {
            return 1.3;
            //return 1.05;
        } else {
            return 0;
        }
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
        addAll(otherScores);
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        String delim = "";
        for (Entry<SimpleEntry<EDGETYPE, VKmer>, Rules> elem : scores.entrySet()) {
            s.append(delim).append("(").append(elem.getKey().getKey()).append(":").append(elem.getKey().getValue())
                    .append(")").append("=").append(elem.getValue());
            delim = ", ";
        }
        return s.toString();
    }
}
