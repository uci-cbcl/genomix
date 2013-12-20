package edu.uci.ics.pregelix.dataflow.util;

import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public class KeyChunkValueUtil {

    public static class ValueTraits implements ITypeTraits {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return false;
        }

        @Override
        public int getFixedLength() {
            return 0;
        }

    }

    public static ITypeTraits[] convertToKeyChunkValueTypeTraits(ITypeTraits[] typeTraits, int[] keyFields) {
        ITypeTraits[] keyChunkValueTraits = new ITypeTraits[keyFields.length + 2];
        for (int i = 0; i < typeTraits.length; ++i) {
            keyChunkValueTraits[i] = typeTraits[keyFields[i]];
        }
        keyChunkValueTraits[keyChunkValueTraits.length - 2] = ChunkId.TypeTrait;
        keyChunkValueTraits[keyChunkValueTraits.length - 1] = new ValueTraits();
        return keyChunkValueTraits;
    }

    public static int[] convertToKeyChunkValueBloomFilterKeyFields(int[] bloomFilterKeyFields, int[] keyFields) {
        int[] filterFields = new int[bloomFilterKeyFields.length];
        int j = 0, k = 0;
        for (int i = 0; i < bloomFilterKeyFields.length; i++) {
            while (j < keyFields.length && bloomFilterKeyFields[i] != keyFields[j]) {
                j++;
            }
            if (j < keyFields.length) {
                filterFields[k++] = j;
            } else {
                throw new IllegalStateException("The bloomFilterKeyFields doesn't match the keyFields");
            }
        }
        return filterFields;
    }

    public static void reset(KeyChunkValueFrameTupleAppender chunkAppender, ArrayTupleBuilder keyBuilder,
            ArrayTupleBuilder valBuilder, FrameTupleReference originTuple, int[] originKeyFields, int chunksize)
            throws HyracksDataException {
        keyBuilder.reset();
        valBuilder.reset();
        int k = 0;
        for (int i = 0; i < originTuple.getFieldCount(); ++i) {
            if (i == originKeyFields[k]) {
                keyBuilder.addField(originTuple.getFieldData(i), originTuple.getFieldStart(i),
                        originTuple.getFieldLength(i));
                k++;
            } else {
                try {
                    valBuilder.getDataOutput().writeInt(originTuple.getFieldLength(i));
                    valBuilder.getDataOutput().write(originTuple.getFieldData(i), originTuple.getFieldStart(i),
                            originTuple.getFieldLength(i));
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new HyracksDataException(e);
                }
            }
        }
        valBuilder.addFieldEndOffset();
        int valuesize = chunksize - keyBuilder.getSize() - ChunkId.TypeTrait.getFixedLength();
        if (keyBuilder.getSize() > chunksize || valuesize < 0) {
            throw new IllegalStateException("The keyfields is too big.");
        }

        ChunkId id = new ChunkId((short) 0);
        int offset = 0;
        do {
            chunkAppender.append(keyBuilder, id, valBuilder.getByteArray(), offset,
                    Math.min(valuesize, valBuilder.getSize() - offset));
            offset += valuesize;
            id.increaseId();
        } while (offset < valBuilder.getSize());
    }
}
