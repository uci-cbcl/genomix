package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.ReadHeadInfo;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;

public class RayValue extends VertexValueWritable {
    private static final long serialVersionUID = 1L;

    Boolean flippedFromInitialDirection = null;
    boolean visited = false;
    boolean intersection = false;
    boolean stopSearch = false;

    protected static class FIELDS {
        public static final byte DIR_FLIPPED_VS_INITIAL = 0b01;
        public static final byte DIR_SAME_VS_INITIAL = 0b10;
        public static final byte VISITED = 0b1 << 2;
        public static final byte INTERSECTION = 0b1 << 3;
        public static final byte STOP_SEARCH = 0b1 << 4;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if (((state) & FIELDS.DIR_FLIPPED_VS_INITIAL) != 0) {
            flippedFromInitialDirection = true;
        } else if (((state) & FIELDS.DIR_SAME_VS_INITIAL) != 0) {
            flippedFromInitialDirection = false;
        } else {
            flippedFromInitialDirection = null;
        }
        visited = ((state & FIELDS.VISITED) != 0);
        intersection = ((state & FIELDS.INTERSECTION) != 0);
        stopSearch = ((state & FIELDS.STOP_SEARCH) != 0);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        state = 0;
        if (flippedFromInitialDirection != null) {
            state |= flippedFromInitialDirection ? FIELDS.DIR_FLIPPED_VS_INITIAL : FIELDS.DIR_SAME_VS_INITIAL;
        }
        if (visited) {
            state |= FIELDS.VISITED;
        }
        if (intersection) {
            state |= FIELDS.INTERSECTION;
        }
        if (stopSearch) {
            state |= FIELDS.STOP_SEARCH;
        }
        super.write(out);
    }

    /**
     * @return whether or not I have any readids that **could** contribute to the current walk
     */
    public boolean isOutOfRange(int myOffset, int walkLength, int maxDist) {
        int numBasesToSkip = Math.max(0, walkLength - maxDist - myOffset);
        int myLength = getKmerLength() - Kmer.getKmerLength() + 1;
        if (!flippedFromInitialDirection) {
            // TODO fix max offset to be distance
            // cut off the beginning
            if (numBasesToSkip > myLength) {
                // start > offsets I contain-- no valid readids
                return true;
            }
            return getUnflippedReadIds().getOffSetRange(numBasesToSkip, ReadHeadInfo.MAX_OFFSET_VALUE).isEmpty();
        } else {
            // cut off the end 
            if (myLength - numBasesToSkip < 0) {
                // my max is negative-- no valid readids
                return true;
            }
            return getFlippedReadIds().getOffSetRange(0, myLength - numBasesToSkip).isEmpty();
        }
    }
}
