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
package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class MergePathValueWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final byte[] EMPTY_BYTES = {};
    private byte adjBitMap;
    private byte flag;
    private VKmerBytesWritable kmer;

    public MergePathValueWritable() {
        this((byte) 0, (byte) 0, 0, EMPTY_BYTES);
    }
    
    public MergePathValueWritable(int k) {
        this.adjBitMap = 0;
        this.flag = 0;
        this.kmer = new VKmerBytesWritable(k);
    }
    
    public MergePathValueWritable(byte adjBitMap, byte flag, int kmerSize, byte[] bytes) {
        this.adjBitMap = adjBitMap;
        this.flag = flag;
        this.kmer = new VKmerBytesWritable(kmerSize, bytes);
        kmer.set(bytes, 0, bytes.length);
    }

    public void set(MergePathValueWritable right) {
        set(right.getAdjBitMap(), right.getFlag(), right.getKmer());
    }

    public void set(byte adjBitMap, byte flag, VKmerBytesWritable kmer) {
        this.kmer.set(kmer);
        this.adjBitMap = adjBitMap;
        this.flag = flag;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        // TODO Auto-generated method stub
        kmer.readFields(arg0);
        adjBitMap = arg0.readByte();
        flag = arg0.readByte();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        // TODO Auto-generated method stub

        kmer.write(arg0);
        arg0.writeByte(adjBitMap);
        arg0.writeByte(flag);
    }

    public VKmerBytesWritable getKmer() {
        if (kmer.getLength() != 0) {
            return kmer;
        }
        return null;
    }

    public byte getAdjBitMap() {
        return this.adjBitMap;
    }

    public byte getFlag() {
        return this.flag;
    }

    public String toString() {
        return GeneCode.getSymbolFromBitMap(adjBitMap) + '\t' + String.valueOf(flag);
    }

    public String pureToString() {
        return GeneCode.getSymbolFromBitMap(adjBitMap);
    }
    @Override
    public byte[] getBytes() {
        // TODO Auto-generated method stub
        if (kmer.getLength() != 0) {
            return kmer.getBytes();
        } else
            return null;

    }

    public int getKmerLength() {
        return kmer.getKmerLength();
    }

    @Override
    public int getLength() {
        return kmer.getLength();
    }
}
