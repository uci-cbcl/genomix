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

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class IPStringParserFactory implements IValueParserFactory {

    private static final long serialVersionUID = 1L;

    private int mask;

    public static final IPStringParserFactory[] FACTORIES = new IPStringParserFactory[33];

    private IPStringParserFactory(int m) {
        mask = m;
    }

    public static final IValueParserFactory getINSTANCE(int m) {
        if (m < 0 || m > 32) {
            throw new IllegalArgumentException("The mask parameter is out of range: " + m);
        }
        if (FACTORIES[m] == null) {
            FACTORIES[m] = new IPStringParserFactory(m);
        }
        return FACTORIES[m];
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory#createValueParser()
     */
    @Override
    public IValueParser createValueParser() {
        return new IValueParser() {

            @Override
            public void parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                int[] ipFields = new int[4];

                int fieldIndex = 0;

                int i = 0;

                while (fieldIndex < 4) {
                    boolean pre = true;
                    for (; pre && i < length; ++i) {
                        char ch = buffer[i + start];
                        switch (ch) {
                            case ' ':
                            case '\t':
                            case '\n':
                            case '\r':
                            case '\f':
                                break;

                            case '0':
                            case '1':
                            case '2':
                            case '3':
                            case '4':
                            case '5':
                            case '6':
                            case '7':
                            case '8':
                            case '9':
                                pre = false;
                                ipFields[fieldIndex] = ipFields[fieldIndex] * 10 + (ch - '0');
                                break;

                            default:
                                throw new HyracksDataException("Encountered " + ch);
                        }
                    }
                    boolean post = false;
                    for (; !post && i < length; ++i) {
                        char ch = buffer[i + start];
                        switch (ch) {
                            case '0':
                            case '1':
                            case '2':
                            case '3':
                            case '4':
                            case '5':
                            case '6':
                            case '7':
                            case '8':
                            case '9':
                                ipFields[fieldIndex] = ipFields[fieldIndex] * 10 + (ch - '0');
                                break;
                            case '.':
                                post = true;
                                break;
                        }
                    }
                    fieldIndex++;
                }

                for (; i < length; ++i) {
                    char ch = buffer[i + start];
                    switch (ch) {
                        case ' ':
                        case '\t':
                        case '\n':
                        case '\r':
                        case '\f':
                            break;

                        default:
                            throw new HyracksDataException("Encountered " + ch);
                    }
                }

                StringBuilder sbder = new StringBuilder();

                if (mask == 32) {
                    for(i = 0; i < 4; i++){
                        sbder.append(ipFields[i]);
                        if(i < 3){
                            sbder.append('.');
                        }
                    }
                } else {

                    int maskedFields = mask >> 3;
                    int maskedOffset = mask & 7;

                    for (i = 0; i < maskedFields; i++) {
                        sbder.append(ipFields[i]);
                        if (i < maskedFields - 1) {
                            sbder.append('.');
                        }
                    }

                    if (maskedOffset != 0) {
                        if (maskedFields > 0)
                            sbder.append('.');
                        sbder.append((ipFields[maskedFields] >> (8 - maskedOffset)) << (8 - maskedOffset));
                    }
                    if (mask > 0)
                        sbder.append('/').append(mask);
                    else
                        sbder.append('*');
                }

                try {
                    out.writeUTF(sbder.toString());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }

}
