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

public class VKmerBytesWritable extends KmerBytesWritable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Deprecated
    public VKmerBytesWritable() {
        super();
    }

    public VKmerBytesWritable(int k, byte[] storage) {
        super(k, storage);
    }

    public VKmerBytesWritable(int k) {
        super(k);
    }

    public VKmerBytesWritable(KmerBytesWritable other) {
        super(other);
    }

    protected void setSize(int size) {
        if (size > getCapacity()) {
            setCapacity((size * 3 / 2));
        }
        this.size = size;
    }

    protected int getCapacity() {
        return bytes.length;
    }

    protected void setCapacity(int new_cap) {
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

    /**
     * Read Kmer from read text into bytes array e.g. AATAG will compress as
     * [0x000G, 0xATAA]
     * 
     * @param k
     * @param array
     * @param start
     */
    public void setByRead(int k, byte[] array, int start) {
        reset(k);
        super.setByRead(array, start);
    }

    /**
     * Compress Reversed Kmer into bytes array AATAG will compress as
     * [0x000A,0xATAG]
     * 
     * @param input
     *            array
     * @param start
     *            position
     */
    public void setByReadReverse(int k, byte[] array, int start) {
        reset(k);
        super.setByReadReverse(array, start);
    }

    @Override
    public void set(KmerBytesWritable newData) {
        if (newData == null){
            this.set(0,null,0,0);
        }else{
            this.set(newData.kmerlength, newData.bytes, 0, newData.size);
        }
    }

    public void set(int k, byte[] newData, int offset, int length) {
        reset(k);
        if (k > 0 ){
            System.arraycopy(newData, offset, bytes, 0, size);
        }
    }

    /**
     * Reset array by kmerlength
     * 
     * @param k
     */
    public void reset(int k) {
        this.kmerlength = k;
        setSize(0);
        setSize(KmerUtil.getByteNumFromK(k));
        clearLeadBit();
    }

}
