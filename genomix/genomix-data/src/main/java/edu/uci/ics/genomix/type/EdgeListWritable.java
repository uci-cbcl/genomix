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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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
    
    public EdgeListWritable(List<EdgeWritable> otherList) {
        this();
        for (EdgeWritable e : otherList) {
            add(e);
        }
    }
    
    public void setAsCopy(EdgeListWritable otherEdge){
        reset();
        for (EdgeWritable e : otherEdge.edges) {
            add(e);
        }
    }

    public void reset() {
        edges.clear();
    }
    
    public EdgeWritable get(int i) {
        return edges.get(i);
    }
    
    public boolean add(EdgeWritable element) {
        return edges.add(new EdgeWritable(element));
    }
    
    public boolean add(VKmerBytesWritable kmer) {
        EdgeWritable edge = new EdgeWritable();
        edge.setKey(kmer);
        return edges.add(edge);
    }
    
    public EdgeWritable set(int i, EdgeWritable element) {
        return edges.set(i, element);
    }
    
    public boolean isEmpty(){
        return getCountOfPosition() == 0;
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
	
    public EdgeWritable getEdge(VKmerBytesWritable key){
        for(EdgeWritable edge : edges){
            if(edge.getKey().equals(key)){
                return edge;
            }
        }
        return null;
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
        Object[] objs = edges.toArray();
        Arrays.sort(objs);
        for (Object e : objs) {
            sbuilder.append(delim).append(e.toString());
            delim = ",";
        }
        sbuilder.append(']');
        return sbuilder.toString();
    }

    @Override
    public Iterator<EdgeWritable> iterator() {
        return edges.iterator();
    }
    
    /**
     * return an iterator over the keys of this edgeList.  Using the iterator.remove() function will remove the entire edge (not just the keys you're iterating over!) 
     */
    public Iterator<VKmerBytesWritable> getKeyIterator() {
        Iterator<VKmerBytesWritable> it = new Iterator<VKmerBytesWritable>() {

            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return currentIndex < edges.size();
            }

            @Override
            public VKmerBytesWritable next() {
                return edges.get(currentIndex++).getKey();
            }

            @Override
            public void remove() {
                edges.remove(--currentIndex);
            }
        };
        return it;
    }
    
    public Iterable<VKmerBytesWritable> getKeys() {
        return new Iterable<VKmerBytesWritable>() {
            @Override
            public Iterator<VKmerBytesWritable> iterator() {
                return getKeyIterator();
            }
        };
    }
    
    public PositionListWritable getReadIDs(VKmerBytesWritable key) {
        for (EdgeWritable e : this) {
            if (e.getKey().equals(key))
                return e.getReadIDs();
        }
        throw new IllegalArgumentException("Cannot get readIDs for VKmer \""+ key +"\". They key was not in the edgeList!");
    }
    
    
    public boolean contains(VKmerBytesWritable toFind){
        Iterator<VKmerBytesWritable> posIterator = this.getKeyIterator();
        while (posIterator.hasNext()) {
            if (toFind.equals(posIterator.next()))
                return true;
        }
        return false;
    }
    
    /*
     * remove the first instance of `toRemove`. Uses a linear scan. Throws an
     * exception if not in this list.
     */
    public void remove(VKmerBytesWritable toRemove, boolean ignoreMissing) {
        Iterator<VKmerBytesWritable> posIterator = this.getKeyIterator();
        while (posIterator.hasNext()) {
            if (toRemove.equals(posIterator.next())) {
                posIterator.remove();
                return; // break as soon as the element is found 
            }
        }
        // element was not found
        if (!ignoreMissing) {
            throw new ArrayIndexOutOfBoundsException("the KmerBytesWritable `" + toRemove.toString()
                    + "` was not found in this list.");
        }
    }

    public void remove(VKmerBytesWritable toRemove) {
        remove(toRemove, false);
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
    
    public void removeSubEdge(EdgeWritable toRemove){
        Iterator<Long> it = toRemove.readIDIter();
        while(it.hasNext()){
            long readId = it.next();
            this.getReadIDs(toRemove.getKey()).removeReadId(readId);
        }
        if(this.getReadIDs(toRemove.getKey()).isEmpty())
            this.remove(toRemove.getKey());
    }

//    public void appendEdgeList(EdgeListWritable other) {
//        for (EdgeWritable e : other.edges) {
//            this.edges.addAll(other.edges.clone());
//        }
//    }
    
    
    /**
     * Adds all edges in edgeList to me.  If I have the same edge as `other`, that entry will be the union of both sets of readIDs.
     * 
     * NOTE: This function may change the order of the original list!
     */
    public void unionUpdate(EdgeListWritable other) {
        // TODO test this function properly
        // TODO perhaps there's a more efficient way to do this?
        HashMap<VKmerBytesWritable, PositionListWritable> unionEdges = new HashMap<VKmerBytesWritable, PositionListWritable>(edges.size() + other.edges.size());
        for (EdgeWritable e : edges) {
            VKmerBytesWritable key = e.getKey();
            if (unionEdges.containsKey(key)) {
//                unionEdges.get(key).unionUpdateCappedCount(e.getReadIDs());
                unionEdges.get(key).unionUpdate(e.getReadIDs());
            }
            else {
                unionEdges.put(key, new PositionListWritable(e.getReadIDs())); // make a new copy of their list
            }
        }
        
        for (EdgeWritable e : other.edges) {
            VKmerBytesWritable key = e.getKey();
            if (unionEdges.containsKey(key)) {
//                unionEdges.get(key).unionUpdateCappedCount(e.getReadIDs());
                unionEdges.get(key).unionUpdate(e.getReadIDs());
            }
            else {
                unionEdges.put(key, new PositionListWritable(e.getReadIDs())); // make a new copy of their list
            }
        }
        edges.clear();
        for (VKmerBytesWritable key : unionEdges.keySet()) {
            edges.add(new EdgeWritable(key, unionEdges.get(key)));
        }
    }

    /**
     * Adds the given edge in to my list. If I have the same key as `other`, that entry will be the union of both sets of readIDs.
     */
    public void unionAdd(EdgeWritable otherEdge) {
        VKmerBytesWritable otherKey = otherEdge.getKey();
        for (EdgeWritable e : this) {
            if (e.getKey().equals(otherKey)) {
                for (PositionWritable p : otherEdge.getReadIDs()) {
                    e.unionAddReadId(p);
                }
                return;
            }
        }
        // didn't find the edge; add a copy of it now
        edges.add(new EdgeWritable(otherEdge));
    }
    
    public void unionAdd(EdgeListWritable otherEdgeList){
        for(EdgeWritable otherEdge : otherEdgeList)
            unionAdd(otherEdge);
    }
}
