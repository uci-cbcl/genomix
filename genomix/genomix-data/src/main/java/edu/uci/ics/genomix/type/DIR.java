package edu.uci.ics.genomix.type;

import java.util.EnumSet;

public enum DIR {

    REVERSE((byte) (0b01 << 2)),
    FORWARD((byte) (0b10 << 2));

    public static final byte MASK = (byte) (0b11 << 2);
    public static final byte CLEAR = (byte) (0b1110011);

    private final byte val;

    private DIR(byte val) {
        this.val = val;
    }

    public final byte get() {
        return val;
    }

    public static DIR mirror(DIR direction) {
        switch (direction) {
            case REVERSE:
                return FORWARD;
            case FORWARD:
                return REVERSE;
            default:
                throw new IllegalArgumentException("Invalid direction given: " + direction);
        }
    }

    public DIR mirror() {
        return mirror(this);
    }

    public static byte fromSet(EnumSet<DIR> set) {
        byte b = 0;
        if (set.contains(REVERSE))
            b |= REVERSE.val;
        if (set.contains(FORWARD))
            b |= FORWARD.val;
        return b;
    }

    public final EnumSet<EDGETYPE> edgeTypes() {
        return edgeTypesInDir(this);
    }

    public static final EnumSet<EDGETYPE> edgeTypesInDir(DIR direction) {
        return direction == DIR.REVERSE ? EDGETYPE.INCOMING : EDGETYPE.OUTGOING;
    }

    public static EnumSet<DIR> enumSetFromByte(short s) { //TODO change shorts to byte? (anbangx) 
        EnumSet<DIR> retSet = EnumSet.noneOf(DIR.class);
        if ((s & REVERSE.get()) != 0)
            retSet.add(DIR.REVERSE);
        if ((s & FORWARD.get()) != 0)
            retSet.add(DIR.FORWARD);
        return retSet;
    }

    /**
     * Given a byte representing NEXT, PREVIOUS, or both, return an enumset representing PREVIOUS, NEXT, or both, respectively.
     */
    public static EnumSet<DIR> flipSetFromByte(short s) {
        EnumSet<DIR> retSet = EnumSet.noneOf(DIR.class);
        if ((s & REVERSE.get()) != 0)
            retSet.add(DIR.FORWARD);
        if ((s & FORWARD.get()) != 0)
            retSet.add(DIR.REVERSE);
        return retSet;
    }

}