package edu.uci.ics.pregelix.dataflow.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class RecoverChunkedTupleBuilder {
    private int[] permutation;
    private ArrayTupleBuilder builder;
    private ArrayTupleReference tempReference;
    private int keyFields;
    private ChunkId chunkId;
    private boolean hadKey;
    private ResetableByteArrayInputStream valueStream;
    private DataInput dataInput;
    private int lengthToRead = -1;
    private byte[] buffer;

    public RecoverChunkedTupleBuilder(int originFieldsCount, int[] originKeyFieldsMap, int chunkSize) {
        permutation = generatePermutation(originFieldsCount, originKeyFieldsMap);
        builder = new ArrayTupleBuilder(originFieldsCount);
        tempReference = new ArrayTupleReference();
        keyFields = originKeyFieldsMap.length;
        chunkId = new ChunkId((short) 0);
        hadKey = false;
        valueStream = new ResetableByteArrayInputStream();
        dataInput = new DataInputStream(valueStream);
        buffer = new byte[chunkSize];

    }

    public static int[] generatePermutation(int originFieldsCount, int[] originKeyFieldsMap) {
        int[] permutation = new int[originFieldsCount];

        int ikey = 0;
        int ival = originKeyFieldsMap.length;
        for (int i = 0; i < originFieldsCount; ++i) {
            if (ikey < originKeyFieldsMap.length && i == originKeyFieldsMap[ikey]) {
                permutation[i] = ikey++;
            } else {
                permutation[i] = ival++;
            }
        }
        if (ikey != originKeyFieldsMap.length || ival != originFieldsCount) {
            throw new IllegalStateException("The provided key fields or count is invalid.");
        }
        return permutation;
    }

    public void reset(ITupleReference keyValueTuple) throws HyracksDataException {
        builder.reset();
        if (keyValueTuple.getFieldCount() == 0) {
            hadKey = false;
            return;
        }
        resetKey(keyValueTuple);
        appendValue(keyValueTuple);
        hadKey = true;
    }

    public void appendValue(ITupleReference chunkTuple) throws HyracksDataException {
        int fieldLength = chunkTuple.getFieldLength(keyFields + 1);
        valueStream.setByteArray(chunkTuple.getFieldData(keyFields + 1), chunkTuple.getFieldStart(keyFields + 1));

        if (builder.getAddedFields() == builder.getFieldEndOffsets().length) {
            //TODO This is the temporary solution to skip the unuseful chunks.
            // e.g. If the original tuple update caused shrink, then the later chunks will not
            // be count any more. We need to write those chunks into delete channel in the near future.  
            return;
        }
        try {
            for (int offset = 0; offset < fieldLength;) {
                if (lengthToRead <= 0) {
                    lengthToRead = dataInput.readInt();
                    offset += 4;
                }

                int readlen = Math.min(lengthToRead, fieldLength - offset);
                dataInput.readFully(buffer, 0, readlen);
                builder.getDataOutput().write(buffer, 0, readlen);
                offset += readlen;
                lengthToRead -= readlen;
                if (lengthToRead <= 0) {
                    builder.addFieldEndOffset();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        };

    }

    public boolean isNextChunk(ITupleReference chunkTuple) throws HyracksDataException {
        if (!hadKey) {
            resetKey(chunkTuple);
            return true;
        }
        boolean sameKey = rawCompareEachKey(builder, chunkTuple);
        if (sameKey) {
            if (!chunkId.checkIfMyNextChunk(chunkTuple.getFieldData(keyFields), chunkTuple.getFieldStart(keyFields))) {
                throw new IllegalStateException("The chunk id sequence is broken.");
            }
        }
        return sameKey;
    }

    private boolean rawCompareEachKey(ArrayTupleBuilder builder2, ITupleReference chunkTuple) {
        tempReference.reset(builder2.getFieldEndOffsets(), builder2.getByteArray());
        for (int i = 0; i < keyFields; ++i) {
            if (!equal(tempReference.getFieldData(i), tempReference.getFieldStart(i), tempReference.getFieldLength(i),
                    chunkTuple.getFieldData(i), chunkTuple.getFieldStart(i), chunkTuple.getFieldLength(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean equal(byte[] fieldData, int fieldStart, int fieldLength, byte[] fieldData2, int fieldStart2,
            int fieldLength2) {
        if (fieldLength != fieldLength2) {
            return false;
        }
        for (int i = 0; i < fieldLength; ++i) {
            if (fieldData[fieldStart + i] != fieldData2[fieldStart2 + i]) {
                return false;
            }
        }
        return true;
    }

    private void resetKey(ITupleReference keyValueTuple) throws HyracksDataException {
        for (int i = 0; i < keyFields; ++i) {
            builder.addField(keyValueTuple.getFieldData(i), keyValueTuple.getFieldStart(i),
                    keyValueTuple.getFieldLength(i));
        }
        chunkId.reset(keyValueTuple.getFieldData(keyFields), keyValueTuple.getFieldStart(keyFields),
                keyValueTuple.getFieldLength(keyFields));
        if (!chunkId.isFirstChunk()) {
            throw new IllegalStateException("The reset tuple is not the first chunk.");
        }
        lengthToRead = -1;
    }

    public int[] getFieldEndOffsets() {
        if (lengthToRead > 0) {
            throw new IllegalStateException("The building process haven't finished yet");
        }
        return builder.getFieldEndOffsets();
    }

    public byte[] getByteArray() {
        if (lengthToRead > 0) {
            throw new IllegalStateException("The building process haven't finished yet");
        }
        return builder.getByteArray();
    }

    public int[] getPermutation() {
        return permutation;
    }

}
