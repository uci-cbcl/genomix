/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash.prepart;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public interface IHashHashSerializableHashTableFactory extends Serializable {

    public IHashHashSpillableFrameWriter createHashTable(IHyracksTaskContext ctx, int framesLimit, int tableSize,
            int numOfPartitions, int[] keys, IBinaryComparator[] comparators, int hashSeedOffset,
            ITuplePartitionComputerFamily tpcFamily, IAggregatorDescriptor aggregator, IAggregatorDescriptor merger,
            RecordDescriptor inputRecordDescriptor, RecordDescriptor outputRecordDescriptor, IFrameWriter outputWriter);

    public double getHashTableFudgeFactor(int hashtableSlots, int estimatedRecordSize, int frameSize,
            int memorySizeInFrames);

    public int getHeaderPages(int hashtableSlots, int frameSize);

}
