package edu.uci.ics.genomix.data;

public class Marshal {
    public static int getInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }
    
    public static void putInt(int val, byte[] bytes, int offset) {
        bytes[offset] = (byte)((val >>> 24) & 0xFF);        
        bytes[offset + 1] = (byte)((val >>> 16) & 0xFF);
        bytes[offset + 2] = (byte)((val >>>  8) & 0xFF);
        bytes[offset + 3] = (byte)((val >>>  0) & 0xFF);
    }
}
