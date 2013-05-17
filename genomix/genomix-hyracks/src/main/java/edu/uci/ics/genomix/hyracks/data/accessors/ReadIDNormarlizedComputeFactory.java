package edu.uci.ics.genomix.hyracks.data.accessors;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class ReadIDNormarlizedComputeFactory implements INormalizedKeyComputerFactory{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        return new INormalizedKeyComputer(){

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                return IntegerSerializerDeserializer.getInt(bytes, start);
            }
            
        };
    }

}
