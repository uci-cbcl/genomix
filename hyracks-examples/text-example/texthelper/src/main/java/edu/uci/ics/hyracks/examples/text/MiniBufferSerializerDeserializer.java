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

import java.io.DataInput;
import java.io.DataOutput;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class MiniBufferSerializerDeserializer implements ISerializerDeserializer<String> {

    private static final long serialVersionUID = 1L;
    
    private final RecordDescriptor recordDescriptor;
    
    private final char delimiter;
    
    private MiniBufferSerializerDeserializer(RecordDescriptor recordDesc, char delimiter){
        this.recordDescriptor = recordDesc;
        this.delimiter = delimiter;
    }
    
    public static MiniBufferSerializerDeserializer getInstance(RecordDescriptor recordDescriptor, char delimiter){
        return new MiniBufferSerializerDeserializer(recordDescriptor, delimiter);
    }

    @Override
    public String deserialize(DataInput in) throws HyracksDataException {
        StringBuilder sbder = new StringBuilder();
        for(int i = 0; i < recordDescriptor.getFieldCount(); i++){
            
        }
        return sbder.toString();
    }

    @Override
    public void serialize(String instance, DataOutput out) throws HyracksDataException {
        int strOffset = 0;
        int fieldIndex = 0;
        while(strOffset < instance.length()){
            int nextMatch = instance.indexOf(delimiter, strOffset);
            
            strOffset = nextMatch + 1;
        }
    }

}
