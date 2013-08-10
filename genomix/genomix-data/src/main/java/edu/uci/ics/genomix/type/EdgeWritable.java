/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.data.KmerUtil;
import edu.uci.ics.genomix.data.Marshal;


public class EdgeWritable implements WritableComparable<EdgeWritable>, Serializable{

    private static final long serialVersionUID = 1L;
    
    private VKmerBytesWritable key;
    private PositionListWritable readIDs;

    public EdgeWritable() {
        key = new VKmerBytesWritable();
        readIDs = new PositionListWritable();
    }
    
    /**
     * Set the internal readIDs when the given positionList has readid, position, and mateid set
     */
    public EdgeWritable(VKmerBytesWritable otherKey, PositionListWritable otherPositions) {
        this();
        setFromPositions(otherKey, otherPositions);
    }
    
    public EdgeWritable(byte[] newStorage, int newOffset) {
        this();
        setAsReference(newStorage, newOffset);
    }
    
    public EdgeWritable(EdgeWritable other) {
        this();
        setFromPositions(other.key, other.readIDs);
    }

    public void setAsCopy(EdgeWritable otherEdge){
        key.setAsCopy(otherEdge.key);
        readIDs.set(otherEdge.readIDs);
    }
    
    /**
     * Set the key and internal readIDs when the given positionList has readid, position, and mateid set
     */
    public void setFromPositions(VKmerBytesWritable otherKey, PositionListWritable otherPositions) {
        key.setAsCopy(otherKey);
        setFromPositions(otherPositions);
    }

    /**
     * Set the internal readIDs when the given positionList has readid, position, and mateid set
     */
    private void setFromPositions(PositionListWritable otherPositions) {
        readIDs.reset();
        for (PositionWritable p : otherPositions) {
            appendIDFromPosition(p);
        }
    }

    public void reset() {
        key.reset(0);
        readIDs.reset();
    }
    
    public VKmerBytesWritable getKey() {
        return key;
    }

    public void setKey(VKmerBytesWritable newKey) {
        key.setAsCopy(newKey);
    }
    
    public PositionListWritable getReadIDs() {
        return readIDs;
    }
    
    /**
     * set my readIDs.  NOTE: this assumes otherReadIDs is already cleared of position and mate info 
     */
    public void setReadIDs(PositionListWritable otherReadIDs) {
        readIDs.set(otherReadIDs);
    }
    
    /**
     * clear the given PositionWritable of its position & mate info, then append it
     */
    public void appendIDFromPosition(PositionWritable posn) {
        readIDs.append((byte)0, posn.getReadId(), 0);
    }
    
    public void appendIDFromPosition(byte mateID, long readID, int posID) {
        readIDs.append((byte)0, readID, 0);
    }
    
    public void appendReadID(long readID) {
        readIDs.append((byte)0, readID, 0);
    }
    
    public long[] readIDArray() {
        return readIDs.toReadIDArray();
    }
    
    public Iterator<Long> readIDIter() {
        Iterator<Long> it = new Iterator<Long>() {
            private int currentIndex = 0;
            
            @Override
            public boolean hasNext() {
                return currentIndex < readIDs.getCountOfPosition();
            }
            
            @Override
            public Long next() {
                return new Long(readIDs.getPosition(currentIndex++).getReadId());
            }
            
            @Override
            public void remove() {
                if (currentIndex <= 0) 
                    throw new IllegalStateException("You must advance the iterator using .next() before calling remove()!");
                readIDs.removePosition(--currentIndex);
            }
        };
        return it;
    }
    
    
    public int getLength() {
        return key.getLength() + readIDs.getLength();
    }
	
	/**
     * Return this Edge's representation as a new byte array 
     */
    public byte[] marshalToByteArray() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(getLength());
        DataOutputStream out = new DataOutputStream(baos);
        write(out);
        return baos.toByteArray();
    }
    
    public void setAsCopy(byte[] data, int offset) {
        int curOffset = offset;
        key.setAsCopy(data, curOffset);
        curOffset += key.getLength();
        readIDs.set(data, curOffset);
    }
    
    public void setAsReference(byte[] data, int offset) {
        int curOffset = offset;
        key.setAsReference(data, curOffset);
        curOffset += key.getLength();
        readIDs.setNewReference(data, curOffset);
    }
	
    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        readIDs.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        key.readFields(in);
        readIDs.readFields(in);
    }

    @Override
    public int compareTo(EdgeWritable other) {
        return this.key.compareTo(other.key);
    }
        
    @Override
    public int hashCode() {
        return this.key.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (! (o instanceof EdgeWritable))
            return false;
            
        EdgeWritable ew = (EdgeWritable) o;
        return key.equals(ew.key) && readIDs.equals(ew.readIDs);
    }
    
    /**
     * this version of toString sorts the readIds so they're a little easier to see
     */
    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        sbuilder.append(key.toString()).append(":[");
        String delim = "";
        long[] ids = readIDs.toReadIDArray();
        Arrays.sort(ids);
        for (long id : ids) {
            sbuilder.append(delim).append(id);
            delim = ",";
        }
        sbuilder.append("]}");
        return sbuilder.toString();
    }
}
