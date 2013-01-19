package edu.uci.ics.hyracks.dataflow.std.group.hashsort;

import java.io.IOException;
import java.util.BitSet;
import java.util.Comparator;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

/**
 * TODO need to be merged with the ReferencedPriorityQueue in the util package
 */
public class ReferencedPriorityQueue {
    private final int frameSize;
    private final RecordDescriptor recordDescriptor;
    private final ReferenceEntryWithBucketID entries[];
    private final int size;
    private final BitSet runAvail;
    private int nItems;

    private final Comparator<ReferenceEntryWithBucketID> comparator;

    public ReferencedPriorityQueue(int frameSize, RecordDescriptor recordDescriptor, int initSize,
            Comparator<ReferenceEntryWithBucketID> comparator) {
        this.frameSize = frameSize;
        this.recordDescriptor = recordDescriptor;
        if (initSize < 1)
            throw new IllegalArgumentException();
        this.comparator = comparator;
        nItems = initSize;
        size = (initSize + 1) & 0xfffffffe;
        entries = new ReferenceEntryWithBucketID[size];
        runAvail = new BitSet(size);
        runAvail.set(0, initSize, true);
        for (int i = 0; i < size; i++) {
            entries[i] = new ReferenceEntryWithBucketID(i, null, -1, -1);
        }
    }

    /**
     * Retrieve the top entry without removing it
     * 
     * @return the top entry
     */
    public ReferenceEntryWithBucketID peek() {
        return entries[0];
    }

    /**
     * compare the new entry with entries within the queue, to find a spot for
     * this new entry
     * 
     * @param entry
     * @return runid of this entry
     * @throws IOException
     */
    public int popAndReplace(FrameTupleAccessor fta, int tIndex, int bucketID) {
        ReferenceEntryWithBucketID entry = entries[0];
        if (entry.getAccessor() == null) {
            entry.setAccessor(new FrameTupleAccessor(frameSize, recordDescriptor));
        }
        entry.getAccessor().reset(fta.getBuffer());
        entry.setTupleIndex(tIndex);
        entry.setBucketID(bucketID);

        add(entry);
        return entry.getRunid();
    }

    /**
     * Push entry into priority queue
     * 
     * @param e
     *            the new Entry
     */
    private void add(ReferenceEntryWithBucketID e) {
        ReferenceEntryWithBucketID min = entries[0];
        int slot = (size >> 1) + (min.getRunid() >> 1);

        ReferenceEntryWithBucketID curr = e;
        while (!runAvail.isEmpty() && slot > 0) {
            int c = 0;
            if (!runAvail.get(entries[slot].getRunid())) {
                // run of entries[slot] is exhausted, i.e. not available, curr
                // wins
                c = 1;
            } else if (entries[slot].getAccessor() != null /*
                                                            * entries[slot] is
                                                            * not MIN value
                                                            */
                    && runAvail.get(curr.getRunid() /* curr run is available */)) {

                if (curr.getAccessor() != null) {
                    c = comparator.compare(entries[slot], curr);
                } else {
                    // curr is MIN value, wins
                    c = 1;
                }
            }

            if (c <= 0) { // curr lost
                // entries[slot] swaps up
                ReferenceEntryWithBucketID tmp = entries[slot];
                entries[slot] = curr;
                curr = tmp;// winner to pass up
            }// else curr wins
            slot >>= 1;
        }
        // set new entries[0]
        entries[0] = curr;
    }

    /**
     * Pop is called only when a run is exhausted
     * 
     * @return
     */
    public ReferenceEntryWithBucketID pop() {
        ReferenceEntryWithBucketID min = entries[0];
        runAvail.clear(min.getRunid());
        add(min);
        nItems--;
        return min;
    }

    public boolean areRunsExhausted() {
        return runAvail.isEmpty();
    }

    public int size() {
        return nItems;
    }
}
