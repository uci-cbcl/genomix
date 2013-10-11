/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.genomix.hadoop.gage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import edu.uci.ics.genomix.util.Marshal;

public class IntPair implements WritableComparable<IntPair> {

    private int first = 0;
    private int second = 0;

    public void set(int left, int right) {
        first = left;
        second = right;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt() + Integer.MIN_VALUE;
        second = in.readInt() + Integer.MIN_VALUE;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first - Integer.MIN_VALUE);
        out.writeInt(second - Integer.MIN_VALUE);
    }

    @Override
    public int hashCode() {
        return first * 157 + second;
    }

    @Override
    public boolean equals(Object right) {
        if (right instanceof IntPair) {
            IntPair r = (IntPair) right;
            return r.first == first && r.second == second;
        } else {
            return false;
        }
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(IntPair.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static { // register this comparator
        WritableComparator.define(IntPair.class, new Comparator());
    }

    @Override
    public int compareTo(IntPair o) {
        if (first != o.first) {
            return first < o.first ? -1 : 1;
        } else if (second != o.second) {
            return second < o.second ? -1 : 1;
        } else {
            return 0;
        }
    }

    public static class FirstPartitioner implements Partitioner<IntPair, IntWritable> {
        @Override
        public int getPartition(IntPair key, IntWritable value, int numPartitions) {
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }

        @Override
        public void configure(JobConf job) {
        }
    }
    public static class FirstGroupingComparator 
                   implements RawComparator<IntPair> {
       @Override
       public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
         return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, 
                                                b2, s2, Integer.SIZE/8);
       }
    
       @Override
       public int compare(IntPair o1, IntPair o2) {
         int l = o1.getFirst();
         int r = o2.getFirst();
         return l == r ? 0 : (l < r ? -1 : 1);
       }
     }
}
