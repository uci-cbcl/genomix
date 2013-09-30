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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.WritableComparable;


public class EdgeWritable implements WritableComparable<EdgeWritable>, Serializable{

    private static final long serialVersionUID = 1L;
    
    public static boolean logReadIds = false;
//    public static int MAX_READ_IDS_PER_EDGE = 250;
    
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
        setAsCopy(otherKey, otherPositions);
    }
    
    public EdgeWritable(byte[] newStorage, int newOffset) {
        this();
        setAsReference(newStorage, newOffset);
    }
    
    public EdgeWritable(EdgeWritable other) {
        this();
        setAsCopy(other.key, other.readIDs);
    }

    public void setAsCopy(EdgeWritable otherEdge){
        key.setAsCopy(otherEdge.key);
        readIDs.set(otherEdge.readIDs);
    }
    
    /**
     * Set the key and internal readIDs when the given positionList has readid, position, and mateid set
     */
    public void setAsCopy(VKmerBytesWritable otherKey, PositionListWritable otherPositions) {
        key.setAsCopy(otherKey);
        setReadIDs(otherPositions);
    }
    
    public void setAsCopy(KmerBytesWritable otherKey, PositionListWritable otherPositions) {
        key.setAsCopy(otherKey);
        setReadIDs(otherPositions);
    }
    
    /**
     * Set the internal readIDs when the given positionList has readid, position, and mateid set
     */
    public void setReadIDs(PositionListWritable otherPositions) {
        readIDs.reset();
        for (PositionWritable p : otherPositions) {
            appendReadID(p);
        }
    }
    
    public void setReadIDs(Set<Long> readIdSet) {
        readIDs.reset();
        for (long p : readIdSet) {
            appendReadID(p);
        }
    }
    
    public void set(VKmerBytesWritable otherKmer, Set<Long> otherReadIds){
        reset();
        setKey(otherKmer);
        setReadIDs(otherReadIds);
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
    
    public Set<Long> getSetOfReadIds(){
        return readIDs.getSetOfReadIds();
    }
        
    /**
     * clear the given PositionWritable of its position & mate info, then append it
     */
    public void appendReadID(PositionWritable posn) {
//        if (readIDs.getCountOfPosition() >= MAX_READ_IDS_PER_EDGE)
//            return;  // past maximum threshold-- just ignore this readid
        readIDs.append((byte)0, posn.getReadId(), 0);
    }
    
    public void unionAddReadId(PositionWritable posn){
//        if (readIDs.getCountOfPosition() >= MAX_READ_IDS_PER_EDGE)
//            return;  // past maximum threshold-- just ignore this readid
        Iterator<Long> it = readIDIter();
        while(it.hasNext()){
            if(it.next() == posn.getReadId())
                return;
        }
        readIDs.append((byte)0, posn.getReadId(), 0);
    }
    
    public void appendReadID(long readID) {
//        if (readIDs.getCountOfPosition() >= MAX_READ_IDS_PER_EDGE)
//            return;  // past maximum threshold-- just ignore this readid
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
    
    public String printReadIdSet(){
        StringBuilder sbuilder = new StringBuilder();
        String delim = "";
        long[] ids = readIDs.toReadIDArray();
        sbuilder.append("[");
        if(ids.length > 0){
            Arrays.sort(ids);
            for(int i = 0; i < ids.length; i++){
                sbuilder.append(delim).append(ids[i]);
                delim = ",";
            }
        }
        sbuilder.append("]");
        return sbuilder.toString();
    }
    /**
     * this version of toString sorts the readIds so they're a little easier to see
     */
    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        sbuilder.append(key.toString());
        if(logReadIds){
            sbuilder.append(":[");
            String delim = "";
            long[] ids = readIDs.toReadIDArray();
            Arrays.sort(ids);
            for (long id : ids) {
                sbuilder.append(delim).append(id);
                delim = ",";
            }
            sbuilder.append("]");
        }
        sbuilder.append("}");
        
        return sbuilder.toString();
    }
    
    public static Set<Long> getEdgeIntersection(EdgeWritable reverseEdge, EdgeWritable forwardEdge){
        Set<Long> edgeIntersection = new HashSet<Long>();
        for (PositionWritable p : reverseEdge.getReadIDs()) {
                edgeIntersection.add(p.getReadId());
        }
        Set<Long> outgoingReadIds = forwardEdge.getSetOfReadIds();
        edgeIntersection.retainAll(outgoingReadIds);
        return edgeIntersection;
    }
    
    @Override
    public int compareTo(EdgeWritable other) {
        return this.key.compareTo(other.key);
    }
}
