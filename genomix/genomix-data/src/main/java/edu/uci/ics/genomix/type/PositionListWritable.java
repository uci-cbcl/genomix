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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.Marshal;

public class PositionListWritable implements Writable, Iterable<PositionWritable>, Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    protected byte[] storage;
    protected int offset;
    protected int valueCount;
    protected static final byte[] EMPTY = {};
    public static final int INTBYTES = 4;
    
    protected PositionWritable posIter = new PositionWritable();

    public PositionListWritable() {
        this.storage = EMPTY;
        this.valueCount = 0;
        this.offset = 0;
    }

    public PositionListWritable(int count, byte[] data, int offset) {
        setNewReference(count, data, offset);
    }
    
    public PositionListWritable(List<PositionWritable> posns) {
        this();
        for (PositionWritable p : posns) {
            append(p);
        }
    }

    public void setNewReference(int count, byte[] data, int offset) {
        this.valueCount = count;
        this.storage = data;
        this.offset = offset;
    }

    protected void setSize(int size) {
        if (size > getCapacity()) {
            setCapacity((size * 3 / 2));
        }
    }

    protected int getCapacity() {
        return storage.length - offset;
    }

    protected void setCapacity(int new_cap) {
        if (new_cap > getCapacity()) {
            byte[] new_data = new byte[new_cap];
            if (storage.length - offset > 0) {
                System.arraycopy(storage, offset, new_data, 0, storage.length - offset);
            }
            storage = new_data;
            offset = 0;
        }
    }

    public PositionWritable getPosition(int i) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        posIter.setNewReference(storage, offset + i * PositionWritable.LENGTH);
        return posIter;
    }

    public void resetPosition(int i, int readID, byte posInRead) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        Marshal.putInt(readID, storage, offset + i * PositionWritable.LENGTH);
        storage[offset + INTBYTES] = posInRead;
    }
    
    @Override
    public Iterator<PositionWritable> iterator() {
        Iterator<PositionWritable> it = new Iterator<PositionWritable>() {

            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return currentIndex < valueCount;
            }

            @Override
            public PositionWritable next() {
                return getPosition(currentIndex++);
            }

            @Override
            public void remove() {
//                for(int cursor = currentIndex; cursor < valueCount; cursor++){
//                    System.arraycopy(storage, offset + cursor * PositionWritable.LENGTH, 
//                            storage, offset + (cursor-1) * PositionWritable.LENGTH, PositionWritable.LENGTH);
//                }
                if(currentIndex < valueCount)
                    System.arraycopy(storage, offset + currentIndex * PositionWritable.LENGTH, 
                          storage, offset + (currentIndex - 1) * PositionWritable.LENGTH, 
                          (valueCount - currentIndex) * PositionWritable.LENGTH);
                valueCount--;
                currentIndex--;
            }
        };
        return it;
    }

    public void set(PositionListWritable list2) {
        set(list2.valueCount, list2.storage, list2.offset);
    }

    public void set(int valueCount, byte[] newData, int offset) {
        this.valueCount = valueCount;
        setSize(valueCount * PositionWritable.LENGTH);
        if (valueCount > 0) {
            System.arraycopy(newData, offset, storage, this.offset, valueCount * PositionWritable.LENGTH);
        }
    }

    public void reset() {
        valueCount = 0;
    }

    public void append(PositionWritable pos) {
        setSize((1 + valueCount) * PositionWritable.LENGTH);
        System.arraycopy(pos.getByteArray(), pos.getStartOffset(), storage, offset + valueCount
                * PositionWritable.LENGTH, pos.getLength());
        valueCount += 1;
    }

    public void append(int readID, byte posInRead) {
        setSize((1 + valueCount) * PositionWritable.LENGTH);
        Marshal.putInt(readID, storage, offset + valueCount * PositionWritable.LENGTH);
        storage[offset + valueCount * PositionWritable.LENGTH + PositionWritable.INTBYTES] = posInRead;
        valueCount += 1;
    }
    
    public static int getCountByDataLength(int length) {
        if (length % PositionWritable.LENGTH != 0) {
            for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
                System.out.println(ste);
            }
            throw new IllegalArgumentException("Length of positionlist is invalid");
        }
        return length / PositionWritable.LENGTH;
    }

    public int getCountOfPosition() {
        return valueCount;
    }

    public byte[] getByteArray() {
        return storage;
    }

    public int getStartOffset() {
        return offset;
    }

    public int getLength() {
        return valueCount * PositionWritable.LENGTH;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.valueCount = in.readInt();
        setSize(valueCount * PositionWritable.LENGTH);
        in.readFully(storage, offset, valueCount * PositionWritable.LENGTH);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(valueCount);
        out.write(storage, offset, valueCount * PositionWritable.LENGTH);
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        for (PositionWritable pos : this) {
            sbuilder.append(pos.toString());
            sbuilder.append(',');
        }
        if (valueCount > 0) {
            sbuilder.setCharAt(sbuilder.length() - 1, ']');
        } else {
            sbuilder.append(']');
        }
        return sbuilder.toString();
    }
    
    @Override
    public int hashCode() {
        return Marshal.hashBytes(getByteArray(), getStartOffset(), getLength());
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PositionListWritable))
            return false;
        PositionListWritable other = (PositionListWritable) o;
        if (this.valueCount != other.valueCount)
            return false;
        for (int i=0; i < this.valueCount; i++) {
                if (!this.getPosition(i).equals(other.getPosition(i)))
                    return false;
        }
        return true;
    }
}
