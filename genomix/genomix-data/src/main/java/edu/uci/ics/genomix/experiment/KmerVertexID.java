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
package edu.uci.ics.genomix.experiment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class KmerVertexID implements Writable {
    private int readID;
    private byte posInRead;

    public KmerVertexID(int readID, byte posInRead) {
        set(readID, posInRead);
    }

    public KmerVertexID() {
        readID = -1;
        posInRead = -1;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        readID = arg0.readInt();
        posInRead = arg0.readByte();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeInt(readID);
        arg0.writeByte(posInRead);
    }

    @Override
    public String toString() {
        return String.valueOf(readID) + '\t' + String.valueOf(posInRead);
    }

    public void set(int readID, byte posInRead) {
        this.readID = readID;
        this.posInRead = posInRead;
    }

    public int getReadID() {
        return readID;
    }

    public void setReadID(int readID) {
        this.readID = readID;
    }

    public byte getPosInRead() {
        return posInRead;
    }
}