/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.genomix.hyracks.data.accessors;

import java.nio.ByteBuffer;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;

public class ReadIDPartitionComputerFactory implements ITuplePartitionComputerFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    public ITuplePartitionComputer createPartitioner() {
        return new ITuplePartitionComputer() {
            @Override
            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) {
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int fieldOffset = accessor.getFieldStartOffset(tIndex, 0);
                int slotLength = accessor.getFieldSlotsLength();

                ByteBuffer buf = accessor.getBuffer();

                int hash = Marshal.getInt(buf.array(), startOffset + fieldOffset + slotLength);
                if (hash < 0) {
                    hash = -(hash + 1);
                }

                return hash % nParts;
            }
        };
    }

}
