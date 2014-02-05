package edu.uci.ics.pregelix.dataflow.util;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;

public class KeyChunkTupleReference implements ITupleReference {

    PermutingTupleReference keyTuple;
    ChunkId chunkId;

    public KeyChunkTupleReference(int keyFields[], short startChunkId) {
        keyTuple = new PermutingTupleReference(keyFields);
        chunkId = new ChunkId(startChunkId);
    }

    public void reset(ITupleReference tuple) {
        keyTuple.reset(tuple);
        chunkId.reset();
    }

    public void increaseChunk() {
        chunkId.increaseId();
    }

    @Override
    public int getFieldCount() {
        return keyTuple.getFieldCount() + 1;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        if (fIdx == keyTuple.getFieldCount()) {
            return chunkId.getFieldData(fIdx);
        }
        return keyTuple.getFieldData(fIdx);
    }

    @Override
    public int getFieldStart(int fIdx) {
        if (fIdx == keyTuple.getFieldCount()) {
            return chunkId.getFieldStart(fIdx);
        }
        return keyTuple.getFieldStart(fIdx);
    }

    @Override
    public int getFieldLength(int fIdx) {
        if (fIdx == keyTuple.getFieldCount()) {
            return chunkId.getFieldLength(fIdx);
        }
        return keyTuple.getFieldLength(fIdx);
    }

}
