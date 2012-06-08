/**
 * 
 */
package edu.uci.ics.hyracks.data.std.accessors;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper implements IBinaryHashFunctionFamily {
    public static final IBinaryHashFunctionFamily INSTANCE = new UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper();

    private static final long serialVersionUID = 1L;

    static final int[] primeCoefficents = { 31, 23, 53, 97, 71, 337, 11, 877, 3, 29 };

    private UTF8StringBinaryHashFunctionFamilyForHybridHashGrouper() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction(int seed) {
        final int coefficient = primeCoefficents[seed % primeCoefficents.length];
        final int r = primeCoefficents[(seed + 1) % primeCoefficents.length];

        return new IBinaryHashFunction() {
            @Override
            public int hash(byte[] bytes, int offset, int length) {
                int h = 0;
                int utflen = UTF8StringPointable.getUTFLen(bytes, offset);
                int sStart = offset + 2;
                int c = 0;

                while (c < utflen) {
                    char ch = UTF8StringPointable.charAt(bytes, sStart + c);
                    h = coefficient % r * h + ch;
                    c += UTF8StringPointable.charSize(bytes, sStart + c);
                }
                if (h == Integer.MIN_VALUE) {
                    h += 1;
                }
                return h;
            }
        };
    }
}
