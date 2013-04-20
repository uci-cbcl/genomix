package edu.uci.ics.genomix.data.std.accessors;

import edu.uci.ics.genomix.data.std.primitive.KmerPointable;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class KmerNormarlizedComputerFactory implements INormalizedKeyComputerFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        return new INormalizedKeyComputer() {
            /**
             * read one int from Kmer, make sure this int is consistent whith Kmer compartor
             */
            @Override
            public int normalize(byte[] bytes, int start, int length) {
                return KmerPointable.getIntReverse(bytes, start, length);
            }
        };
    }
}