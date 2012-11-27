/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.data.std.accessors;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;

public class ByteBasedBinaryHashFunctionFamily implements IBinaryHashFunctionFamily {
    public static final IBinaryHashFunctionFamily INSTANCE = new ByteBasedBinaryHashFunctionFamily();

    private static final long serialVersionUID = 1L;

    static final int[] primeCoefficents = { 1073741741, 31, 536870869, 947, 1073741783, 337, 536870951, 53, 877, 71,
            757, 11, 599, 89 };

    private ByteBasedBinaryHashFunctionFamily() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction(int seed) {
        final int coefficient = primeCoefficents[seed % primeCoefficents.length];
        //final int r = primeCoefficents[(seed + 1) % primeCoefficents.length];

        return new IBinaryHashFunction() {
            @Override
            public int hash(byte[] bytes, int offset, int length) {
                int h = 0;

                for (int i = offset; i < offset + length; i++) {
                    h = coefficient * h + bytes[i];
                }

                if (h == Integer.MIN_VALUE) {
                    h += 1;
                }
                return h;
            }
        };
    }
}
