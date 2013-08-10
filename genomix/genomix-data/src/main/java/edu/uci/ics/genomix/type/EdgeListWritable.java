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

import edu.uci.ics.genomix.data.Marshal;


public class EdgeListWritable implements WritableComparable<EdgeListWritable>, Serializable, Iterable<EdgeWritable>{

    private static final long serialVersionUID = 1L;
    private static final int SIZE_INT = 4;
    
    protected EdgeWritable edgeIter = new EdgeWritable();
    
    private ArrayList<EdgeWritable> edges;

    public EdgeListWritable() {
        edges = new ArrayList<EdgeWritable>(1);
    }
    
    /**
     * Set the internal readIDs when the given positionList has readid, position, and mateid set
     */
    public EdgeListWritable(EdgeListWritable other) {
        this();
        setAsCopy(other);
    }
    
    public void setAsCopy(EdgeListWritable otherEdge){
        reset();
        edges.addAll(otherEdge.edges);
    }

    public void reset() {
        edges.clear();
    }
    
    public EdgeWritable get(int i) {
        return edges.get(i);
    }
    
    public boolean add(EdgeWritable element) {
        return edges.add(element);
    }
    
    public EdgeWritable set(int i, EdgeWritable element) {
        return edges.set(i, element);
    }
    
    public int getCountOfPosition() {
        return edges.size();
    }
        
    public int getLength() {
        int total = SIZE_INT;
        for (EdgeWritable e : edges) {
            total += e.getLength();
        }
        return total;
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
        int count = Marshal.getInt(data, offset);
        curOffset += SIZE_INT;
        edges.clear();
        for (int i=0; i < count; i++) {
            EdgeWritable e = new EdgeWritable();
            e.setAsCopy(data, curOffset);
            edges.add(e);
            curOffset += e.getLength();
        }
    }
    
    public void setAsReference(byte[] data, int offset) {
        int curOffset = offset;
        int count = Marshal.getInt(data, offset);
        curOffset += SIZE_INT;
        edges.clear();
        for (int i=0; i < count; i++) {
            edges.add(new EdgeWritable(data, curOffset));
            curOffset += edges.get(i).getLength();
        }
    }
	
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(edges.size());
        for (EdgeWritable e : edges) {
            e.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            EdgeWritable e = new EdgeWritable();
            e.readFields(in);
            edges.add(e);
        }
    }

    /**
     * initial comparison is based on the edgelist length, then equivalence of the edges' kmers
     */
    @Override
    public int compareTo(EdgeListWritable other) {
        int result = Integer.compare(edges.size(), other.edges.size()); 
        if (result != 0) {
            return result;
        }
        for (int i=0; i < edges.size(); i++) {
            result = edges.get(i).compareTo(other.edges.get(i));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }
        
    @Override
    public int hashCode() {
        return edges.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (! (o instanceof EdgeListWritable))
            return false;
            
        EdgeListWritable ew = (EdgeListWritable) o;
        return compareTo(ew) == 0;
    }
    
    /**
     * this version of toString sorts the readIds so they're a little easier to see
     */
    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        String delim = "";
        for (EdgeWritable e : edges) {
            sbuilder.append(delim).append(e);
            delim = ", ";
        }
        sbuilder.append(']');
        return sbuilder.toString();
    }

    @Override
    public Iterator<EdgeWritable> iterator() {
        return edges.iterator();
    }

    /*
     * remove the first instance of @toRemove. Uses a linear scan.  Throws an exception if not in this list.
     */
    public void remove(EdgeWritable toRemove, boolean ignoreMissing) {
        Iterator<EdgeWritable> edgeIterator = this.iterator();
        while (edgeIterator.hasNext()) {
            if (toRemove.equals(edgeIterator.next())) {
                edgeIterator.remove();
                return;  // found it. return early. 
            }
        }
        // element not found.
        if (!ignoreMissing) {
            throw new ArrayIndexOutOfBoundsException("the EdgeWritable `" + toRemove.toString()
                    + "` was not found in this list.");
        }
    }

    public void remove(EdgeWritable toRemove) {
        remove(toRemove, false);
    }
}
