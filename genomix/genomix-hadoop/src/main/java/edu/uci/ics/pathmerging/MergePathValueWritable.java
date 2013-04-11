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
package edu.uci.ics.pathmerging;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.type.Kmer;

public class MergePathValueWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final byte[] EMPTY_BYTES = {};
    private int size;
    private byte[] bytes;

    private byte adjBitMap;
    private byte flag;
    private int kmerSize;

    public MergePathValueWritable() {
        this((byte) 0, (byte) 0, (byte) 0, EMPTY_BYTES);
    }

    public MergePathValueWritable(byte adjBitMap, byte flag, byte kmerSize, byte[] bytes) {
        this.adjBitMap = adjBitMap;
        this.flag = flag;
        this.kmerSize = kmerSize;

        this.bytes = bytes;
        this.size = bytes.length;
    }

    public void setSize(int size) {
        if (size > getCapacity()) {
            setCapacity(size * 3 / 2);
        }
        this.size = size;
    }

    public int getCapacity() {
        return bytes.length;
    }

    public void setCapacity(int new_cap) {
        if (new_cap != getCapacity()) {
            byte[] new_data = new byte[new_cap];
            if (new_cap < size) {
                size = new_cap;
            }
            if (size != 0) {
                System.arraycopy(bytes, 0, new_data, 0, size);
            }
            bytes = new_data;
        }
    }

    public void set(MergePathValueWritable newData) {
        set(newData.bytes, 0, newData.size, newData.adjBitMap, newData.flag, newData.kmerSize);
    }

    public void set(byte[] newData, int offset, int length, byte adjBitMap, byte flag, int kmerSize) {
        setSize(0);        
        if (length != 0) {
            setSize(length);
            System.arraycopy(newData, offset, bytes, 0, size);
        }
            this.adjBitMap = adjBitMap;
            this.flag = flag;
            this.kmerSize = kmerSize;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        // TODO Auto-generated method stub
        setSize(0); // clear the old data
        setSize(arg0.readInt());
        if(size != 0){
        arg0.readFully(bytes, 0, size);
        }
        adjBitMap = arg0.readByte();
        flag = arg0.readByte();
        kmerSize = arg0.readInt();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        // TODO Auto-generated method stub
        arg0.writeInt(size);
        arg0.write(bytes, 0, size);
        arg0.writeByte(adjBitMap);
        arg0.writeByte(flag);
        arg0.writeInt(kmerSize);
    }

    @Override
    public byte[] getBytes() {
        // TODO Auto-generated method stub
        return bytes;
    }

    @Override
    public int getLength() {
        // TODO Auto-generated method stub
        return size;
    }

    public byte getAdjBitMap() {
        return this.adjBitMap;
    }

    public byte getFlag() {
        return this.flag;
    }

    public int getKmerSize() {
        return this.kmerSize;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer(3 * size);
        for (int idx = 0; idx < size; idx++) {
            // if not the first, put a blank separator in
            if (idx != 0) {
                sb.append(' ');
            }
            String num = Integer.toHexString(0xff & bytes[idx]);
            // if it is only one digit, add a leading 0.
            if (num.length() < 2) {
                sb.append('0');
            }
            sb.append(num);
        }
        return Kmer.GENE_CODE.getSymbolFromBitMap(adjBitMap) + '\t' + String.valueOf(flag) + '\t' + sb.toString();
    }
}
