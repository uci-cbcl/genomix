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
package edu.uci.ics.genomix.hadoop.oldtype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class KmerCountValue implements Writable {
    private byte adjBitMap;
    private byte count;

    public KmerCountValue(byte bitmap, byte count) {
        set(bitmap, count);
    }

    public KmerCountValue() {
        adjBitMap = 0;
        count = 0;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        adjBitMap = arg0.readByte();
        count = arg0.readByte();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeByte(adjBitMap);
        arg0.writeByte(count);
    }

    @Override
    public String toString() {
        return GeneCode.getSymbolFromBitMap(adjBitMap) + '\t' + String.valueOf(count);
    }

    public void set(byte bitmap, byte count) {
        this.adjBitMap = bitmap;
        this.count = count;
    }

    public byte getAdjBitMap() {
        return adjBitMap;
    }

    public void setAdjBitMap(byte adjBitMap) {
        this.adjBitMap = adjBitMap;
    }

    public byte getCount() {
        return count;
    }
}