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
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.Marshal;

public class EdgeListWritable extends TreeMap<VKmerBytesWritable, ReadIdListWritable> implements Writable, Serializable {

    private static final long serialVersionUID = 1L;
    private static final int SIZE_INT = 4;
    public static boolean logReadIds; // FIXME regression in usage of this (I broke it)

    public EdgeListWritable() {
        super();
    }

    /**
     * Set the internal readIDs when the given positionList has readid, position, and mateid set
     */
    public EdgeListWritable(EdgeListWritable other) {
        super();
        setAsCopy(other);
    }

    //    public EdgeListWritable(List<Entry<VKmerBytesWritable, ReadIdListWritable>> list) {
    public EdgeListWritable(List<SimpleEntry<VKmerBytesWritable, ReadIdListWritable>> list) {
        super();
        for (Entry<VKmerBytesWritable, ReadIdListWritable> e : list) {
            put(e.getKey(), e.getValue());
        }
    }

    //    public EdgeListWritable(List<SimpleEntry<VKmerBytesWritable, ReadIdListWritable>> asList) {
    //        // TODO Auto-generated constructor stub
    //    }

    public void setAsCopy(EdgeListWritable other) {
        clear();
        for (Entry<VKmerBytesWritable, ReadIdListWritable> e : other.entrySet()) {
            put(e.getKey(), new ReadIdListWritable(e.getValue()));
        }
    }

    public int getLengthInBytes() {
        int total = SIZE_INT;
        for (Entry<VKmerBytesWritable, ReadIdListWritable> e : entrySet()) {
            total += e.getKey().getLength() + e.getValue().getLengthInBytes();
        }
        return total;
    }

    /**
     * Return this Edge's representation as a new byte array
     */
    public byte[] marshalToByteArray() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(getLengthInBytes());
        DataOutputStream out = new DataOutputStream(baos);
        write(out);
        return baos.toByteArray();
    }

    public void setAsCopy(byte[] data, int offset) {
        int curOffset = offset;
        int count = Marshal.getInt(data, offset);
        curOffset += SIZE_INT;
        clear();
        for (int i = 0; i < count; i++) {
            VKmerBytesWritable kmer = new VKmerBytesWritable();
            kmer.setAsCopy(data, curOffset);
            curOffset += kmer.getLength();

            ReadIdListWritable ids = new ReadIdListWritable();
            ids.setAsCopy(data, curOffset);
            curOffset += ids.getLengthInBytes();

            put(kmer, ids);
        }
    }

    public void setAsReference(byte[] data, int offset) {
        int curOffset = offset;
        int count = Marshal.getInt(data, offset);
        curOffset += SIZE_INT;
        clear();
        for (int i = 0; i < count; i++) {
            VKmerBytesWritable kmer = new VKmerBytesWritable();
            kmer.setAsReference(data, curOffset);
            curOffset += kmer.getLength();

            ReadIdListWritable ids = new ReadIdListWritable();
            ids.setAsCopy(data, curOffset);
            curOffset += ids.getLengthInBytes();

            put(kmer, ids);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size());
        for (Entry<VKmerBytesWritable, ReadIdListWritable> e : entrySet()) {
            e.getKey().write(out);
            e.getValue().write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            VKmerBytesWritable kmer = new VKmerBytesWritable();
            kmer.readFields(in);
            ReadIdListWritable ids = new ReadIdListWritable();
            ids.readFields(in);
        }
    }

    public void removeReadIdSubset(VKmerBytesWritable kmer, ReadIdListWritable readIdsToRemove) {
        ReadIdListWritable curReadIds = get(kmer);
        if (curReadIds == null) {
            throw new IllegalArgumentException(
                    "Tried to remove a readId subset for a Kmer that's not in this list!\nTried to remove: " + kmer
                            + "(" + readIdsToRemove + ")" + "\n My edges are: " + this);
        }
        curReadIds.removeAll(readIdsToRemove);
        if (curReadIds.isEmpty()) {
            remove(kmer);
        }
    }

    /**
     * Adds all edges in edgeList to me. If I have the same edge as `other`, that entry will be the union of both sets of readIDs.
     */
    public void unionUpdate(EdgeListWritable other) {
        for (Entry<VKmerBytesWritable, ReadIdListWritable> e : other.entrySet()) {
            unionAdd(e.getKey(), e.getValue());
        }
    }

    /**
     * Adds the given edge in to my list. If I have the same key as `other`, that entry will be the union of both sets of readIDs.
     */
    public void unionAdd(VKmerBytesWritable kmer, ReadIdListWritable readIds) {
        if (containsKey(kmer)) {
            get(kmer).addAll(readIds);
        } else {
            put(kmer, new ReadIdListWritable(readIds));
        }
    }
}
