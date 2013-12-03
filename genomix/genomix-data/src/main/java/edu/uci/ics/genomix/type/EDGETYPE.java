package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.io.Writable;

public enum EDGETYPE implements Writable{ 

    FF((byte) (0b00)),
    FR((byte) (0b01)),
    RF((byte) (0b10)),
    RR((byte) (0b11));

    public static final byte MASK = (byte) (0b11 << 0);
    public static final byte CLEAR = (byte) (0b1111100 << 0);
    private byte val;

    private EDGETYPE(byte val) {
        this.val = val;
    }

    public final byte get() {
        return val;
    }

    public static final EnumSet<EDGETYPE> REVERSE = EnumSet.of(RF, RR);
    public static final EnumSet<EDGETYPE> FORWARD = EnumSet.of(FF, FR);

    public static EDGETYPE fromByte(short b) {
        b &= MASK;
        if (b == FF.val)
            return FF;
        if (b == FR.val)
            return FR;
        if (b == RF.val)
            return RF;
        if (b == RR.val)
            return RR;
        return null;

    }

    /**
     * Returns the edge dir for B->A when the A->B edge is type @dir
     */
    public EDGETYPE mirror() {
        return mirror(this);
    }

    public static EDGETYPE mirror(EDGETYPE edgeType) {
        switch (edgeType) {
            case FF:
                return RR;
            case FR:
                return FR;
            case RF:
                return RF;
            case RR:
                return FF;
            default:
                throw new RuntimeException("Unrecognized direction in mirrorDirection: " + edgeType);
        }
    }
    
    /**
     * 
     */
    public static EDGETYPE getEdgeTypeFromDirToDir(DIR dir1, DIR dir2){
        switch(dir1){
            case FORWARD:
                switch(dir2){
                    case FORWARD:
                        return FF;
                    case REVERSE:
                        return FR;
                    default:
                        throw new IllegalArgumentException("Invalid direction2 given: " + dir2);
                }
            case REVERSE:
                switch(dir2){
                    case FORWARD:
                        return RF;
                    case REVERSE:
                        return RR;
                    default:
                        throw new IllegalArgumentException("Invalid direction2 given: " + dir2);
                }
            default:
                throw new IllegalArgumentException("Invalid direction1 given: " + dir2);
        }
    }
    
    public DIR dir() {
        return dir(this);
    }

    public static DIR dir(EDGETYPE edgeType) { // .dir static / non-static
        switch (edgeType) {
            case FF:
            case FR:
                return DIR.FORWARD;
            case RF:
            case RR:
                return DIR.REVERSE;
            default:
                throw new RuntimeException("Unrecognized direction in dirFromEdgeType: " + edgeType);
        }
    }

    /**
     * return the edgetype corresponding to moving across edge1 and edge2.
     * So if A <-e1- B -e2-> C, we will return the relationship from A -> C
     * If the relationship isn't a valid path (e.g., e1,e2 are both FF), an exception is raised.
     */
    public static EDGETYPE resolveEdgeThroughPath(EDGETYPE BtoA, EDGETYPE BtoC) {
        EDGETYPE AtoB = mirror(BtoA);
        // a valid path must exist from A to C
        // specifically, two rules apply for AtoB and BtoC
        //      1) the internal letters must be the same (so FF, RF will be an error)
        //      2) the final direction is the 1st letter of AtoB + 2nd letter of BtoC
        // TODO? maybe we could use the string version to resolve this following above rules
        switch (AtoB) {
            case FF:
                switch (BtoC) {
                    case FF:
                    case FR:
                        return BtoC;
                    case RF:
                    case RR:
                        throw new IllegalArgumentException("Tried to resolve an invalid link type: A --" + AtoB
                                + "--> B --" + BtoC + "--> C");
                }
                break;
            case FR:
                switch (BtoC) {
                    case FF:
                    case FR:
                        throw new IllegalArgumentException("Tried to resolve an invalid link type: A --" + AtoB
                                + "--> B --" + BtoC + "--> C");
                    case RF:
                        return FF;
                    case RR:
                        return FR;
                }
                break;
            case RF:
                switch (BtoC) {
                    case FF:
                        return RF;
                    case FR:
                        return RR;
                    case RF:
                    case RR:
                        throw new IllegalArgumentException("Tried to resolve an invalid link type: A --" + AtoB
                                + "--> B --" + BtoC + "--> C");
                }
                break;
            case RR:
                switch (BtoC) {
                    case FF:
                    case FR:
                        throw new IllegalArgumentException("Tried to resolve an invalid link type: A --" + AtoB
                                + "--> B --" + BtoC + "--> C");
                    case RF:
                        return RF;
                    case RR:
                        return RR;
                }
                break;
        }
        throw new IllegalStateException("Logic Error or unrecognized direction... original values were: " + BtoA
                + " and " + BtoC);
    }

    public boolean causesFlip() {
        return causesFlip(this);
    }

    public static boolean causesFlip(EDGETYPE edgeType) {
        switch (edgeType) {
            case FF:
            case RR:
                return false;
            case FR:
            case RF:
                return true;
            default:
                throw new IllegalArgumentException("unrecognized direction: " + edgeType);
        }
    }

    public EDGETYPE flipNeighbor() {
        return flipNeighbor(this);
    }

    public static EDGETYPE flipNeighbor(EDGETYPE neighborToMe) {
        switch (neighborToMe) {
            case FF:
                return FR;
            case FR:
                return FF;
            case RF:
                return RR;
            case RR:
                return RF;
            default:
                throw new RuntimeException("Unrecognized direction for neighborDir: " + neighborToMe);
        }
    }

    public static boolean sameOrientation(EDGETYPE et1, EDGETYPE et2) {
        return et1.causesFlip() == et2.causesFlip();
    }

    public static boolean sameOrientation(byte b1, byte b2) {
        EDGETYPE et1 = EDGETYPE.fromByte(b1);
        EDGETYPE et2 = EDGETYPE.fromByte(b2);
        return sameOrientation(et1, et2);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(this.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.val = in.readByte();
    }
}
