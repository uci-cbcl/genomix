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
package edu.uci.ics.hyracks.comm.channels;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.net.protocols.muxdemux.IBufferFactory;

/**
 * @author yingyib
 */
public class WriteBufferFactory implements IBufferFactory {

    private int counter = 0;
    private final PartitionId partitionId;
    private final int initMessageSize;

    public WriteBufferFactory(PartitionId partitionId, int initMessageSize) {
        this.partitionId = partitionId;
        this.initMessageSize = initMessageSize;
    }

    @Override
    public ByteBuffer createBuffer() {
        try {
            if (counter > 1) {
                return null;
            } else {
                ByteBuffer writeBuffer = ByteBuffer.allocate(initMessageSize);
                writeBuffer.putLong(partitionId.getJobId().getId());
                writeBuffer.putInt(partitionId.getConnectorDescriptorId().getId());
                writeBuffer.putInt(partitionId.getSenderIndex());
                writeBuffer.putInt(partitionId.getReceiverIndex());
                writeBuffer.flip();
                counter++;
                return writeBuffer;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
