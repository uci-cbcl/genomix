package edu.uci.ics.genomix.pregelix.io.common;

public abstract interface ByteBacked<E> {
    public byte getByte();
    public E fromByte(byte b);
}
