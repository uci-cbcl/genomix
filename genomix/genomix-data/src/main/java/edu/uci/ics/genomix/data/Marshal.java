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
package edu.uci.ics.genomix.data;

import java.nio.ByteBuffer;

public class Marshal {
    public static int getInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }
    
    public static long getLong(byte[] bytes, int offset) {
        
        long value = 0;
        for (int i = offset; i < bytes.length && i < offset + 8; i++)
        {
           value = (value << 8) + (bytes[i] & 0xff);
        }
        return value;
//        return ((bytes[offset] & 0xff) << 56) + ((bytes[offset + 1] & 0xff) << 48) + ((bytes[offset + 2] & 0xff) << 40) 
//                + ((bytes[offset + 3] & 0xff) << 32) + ((bytes[offset + 4] & 0xff) << 24) + ((bytes[offset + 5] & 0xff) << 16) 
//                + ((bytes[offset + 6] & 0xff) << 8) + ((bytes[offset + 7] & 0xff) << 0);
    }
    
    public static void putInt(int val, byte[] bytes, int offset) {
        bytes[offset] = (byte)((val >>> 24) & 0xFF);        
        bytes[offset + 1] = (byte)((val >>> 16) & 0xFF);
        bytes[offset + 2] = (byte)((val >>>  8) & 0xFF);
        bytes[offset + 3] = (byte)((val >>>  0) & 0xFF);
    }
    
    public static void putLong(long val, byte[] bytes, int offset) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8); 
        //byte[] tmp = byteBuffer.putLong(val).array();
        System.arraycopy(byteBuffer.putLong(val).array(), 0, bytes, offset, 8);
//        bytes[offset] = (byte)((val >>> 56) & 0xFF);        
//        bytes[offset + 1] = (byte)((val >>> 48) & 0xFF);
//        bytes[offset + 2] = (byte)((val >>> 40) & 0xFF);
//        bytes[offset + 3] = (byte)((val >>> 32) & 0xFF);
//        bytes[offset + 4] = (byte)((val >>> 24) & 0xFF);
//        bytes[offset + 5] = (byte)((val >>> 16) & 0xFF);
//        bytes[offset + 6] = (byte)((val >>> 8) & 0xFF);
//        bytes[offset + 7] = (byte)((val >>> 0) & 0xFF);
    }
    
    public static int hashBytes(byte[] bytes, int offset, int length) {
        int hash = 1;
        for (int i = offset; i < offset + length; i++)
            hash = (31 * hash) + (int) bytes[i];
        return hash;
    }
}
