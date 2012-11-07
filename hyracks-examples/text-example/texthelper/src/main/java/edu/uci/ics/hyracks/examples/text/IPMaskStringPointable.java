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
package edu.uci.ics.hyracks.examples.text;

import java.util.regex.Pattern;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IComparable;
import edu.uci.ics.hyracks.data.std.api.IHashable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;

public class IPMaskStringPointable extends AbstractPointable implements IHashable, IComparable {

    private static final Pattern SPLIT_PATTERN = Pattern.compile("\\.");

    private int mask;

    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return false;
        }

        @Override
        public int getFixedLength() {
            return 0;
        }
    };

    private IPMaskStringPointable(int mask) {
        this.mask = mask;
    }

    public static final IPointableFactory[] FACTORIES = new IPointableFactory[33];

    public static IPointableFactory getFactory(final int mask) {
        if (mask < 0 || mask > 32)
            throw new IllegalArgumentException("The mask parameter is out of range: " + mask);
        if (FACTORIES[mask] == null) {
            FACTORIES[mask] = new IPointableFactory() {

                private static final long serialVersionUID = 1L;

                @Override
                public ITypeTraits getTypeTraits() {
                    return TYPE_TRAITS;
                }

                @Override
                public IPointable createPointable() {
                    return new IPMaskStringPointable(mask);
                }
            };
        }
        return FACTORIES[mask];
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.data.std.api.IComparable#compareTo(edu.uci.ics.hyracks.data.std.api.IPointable)
     */
    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.data.std.api.IComparable#compareTo(byte[], int, int)
     */
    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        int utflen1 = getUTFLen(this.bytes, this.start);
        int utflen2 = getUTFLen(bytes, start);

        int s1Start = this.start + 2;
        int s2Start = start + 2;

        StringBuilder sbder = new StringBuilder();
        String[] items;
        int[] thisIP = new int[4];
        int[] thatIP = new int[4];

        for (int i = 0; i < utflen1; i++) {
            sbder.append(charAt(this.bytes, s1Start + i));
        }
        items = SPLIT_PATTERN.split(sbder.toString());
        for (int i = 0; i < items.length; i++) {
            thisIP[i] = Integer.valueOf(items[i]);
        }

        sbder = new StringBuilder();
        for (int i = 0; i < utflen2; i++) {
            sbder.append(charAt(bytes, s2Start + i));
        }
        items = SPLIT_PATTERN.split(sbder.toString());
        for (int i = 0; i < items.length; i++) {
            thatIP[i] = Integer.valueOf(items[i]);
        }

        int maskedFields = mask >> 3;
        int maskedOffset = mask & 7;

        for (int i = 0; i < maskedFields; i++) {
            if (thisIP[i] > thatIP[i]) {
                return 1;
            } else if (thisIP[i] < thatIP[i]) {
                return -1;
            }
        }

        int tail1 = thisIP[maskedFields] >> (8 - maskedOffset);
        int tail2 = thatIP[maskedFields] >> (8 - maskedOffset);

        if (tail1 > tail2) {
            return 1;
        } else if (tail1 < tail2) {
            return -1;
        }

        return 0;
    }

    public static int getUTFLen(byte[] b, int s) {
        return ((b[s] & 0xff) << 8) + ((b[s + 1] & 0xff) << 0);
    }

    public static char charAt(byte[] b, int s) {
        int c = b[s] & 0xff;
        switch (c >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return (char) c;

            case 12:
            case 13:
                return (char) (((c & 0x1F) << 6) | ((b[s + 1]) & 0x3F));

            case 14:
                return (char) (((c & 0x0F) << 12) | (((b[s + 1]) & 0x3F) << 6) | (((b[s + 2]) & 0x3F) << 0));

            default:
                throw new IllegalArgumentException();
        }
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.data.std.api.IHashable#hash()
     */
    @Override
    public int hash() {
        int utflen1 = getUTFLen(this.bytes, this.start);

        int s1Start = this.start + 2;

        StringBuilder sbder = new StringBuilder();
        String[] items;
        int[] thisIP = new int[4];

        for (int i = 0; i < utflen1; i++) {
            sbder.append(charAt(this.bytes, s1Start + i));
        }
        items = SPLIT_PATTERN.split(sbder.toString());
        for (int i = 0; i < items.length; i++) {
            thisIP[i] = Integer.valueOf(items[i]);
        }

        int h = 0;

        for (int i : thisIP) {
            h = 31 * h + i;
        }

        return h;
    }

}
