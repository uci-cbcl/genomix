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
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;

public class HybridHashPartitionGenerateFrameWriter implements IFrameWriter {
    private final IHyracksTaskContext ctx;

    private RunFileWriter[] partitions;

    private int[] partitionRawSizesInFrames;
    private int[] partitionRawSizesInTuples;

    private List<IFrameReader> partitionRunReaders;
    private List<Integer> partitionRunAggregatedPages;
    private List<Integer> partitionSizeInFrames;
    private List<Integer> partitionSizeInTuples;

    private ByteBuffer[] outputBuffers;

    private final int numOfPartitions;

    private final ITuplePartitionComputer tpc;

    private final FrameTupleAccessor inFrameTupleAccessor;

    private final FrameTupleAppender outFrameTupleAppender;

    public HybridHashPartitionGenerateFrameWriter(IHyracksTaskContext ctx, int numOfPartitions,
            ITuplePartitionComputer tpc, RecordDescriptor inRecDesc) {
        this.ctx = ctx;
        this.numOfPartitions = numOfPartitions;
        this.tpc = tpc;
        this.partitions = new RunFileWriter[numOfPartitions];
        this.outputBuffers = new ByteBuffer[numOfPartitions];
        this.partitionRawSizesInTuples = new int[numOfPartitions];
        this.partitionRawSizesInFrames = new int[numOfPartitions];
        this.inFrameTupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);
        this.outFrameTupleAppender = new FrameTupleAppender(ctx.getFrameSize());
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#open()
     */
    @Override
    public void open() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inFrameTupleAccessor.reset(buffer);
        int tupleCount = inFrameTupleAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            int pid = tpc.partition(inFrameTupleAccessor, i, numOfPartitions);

            ctx.getCounterContext().getCounter("optional.partition.insert.count", true).update(1);

            if (outputBuffers[pid] == null) {
                outputBuffers[pid] = ctx.allocateFrame();
            }
            outFrameTupleAppender.reset(outputBuffers[pid], false);

            if (!outFrameTupleAppender.append(inFrameTupleAccessor, i)) {
                // flush the output buffer
                if (partitions[pid] == null) {
                    partitions[pid] = new RunFileWriter(ctx.getJobletContext().createManagedWorkspaceFile(
                            HybridHashPartitionGenerateFrameWriter.class.getSimpleName()), ctx.getIOManager());
                    partitions[pid].open();
                }
                FrameUtils.flushFrame(outputBuffers[pid], partitions[pid]);
                partitionRawSizesInFrames[pid]++;
                outFrameTupleAppender.reset(outputBuffers[pid], true);
                if (!outFrameTupleAppender.append(inFrameTupleAccessor, i)) {
                    throw new HyracksDataException(
                            "Failed to insert a record into its partition: the record size is too large. ");
                }
            }
            partitionRawSizesInTuples[pid]++;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#fail()
     */
    @Override
    public void fail() throws HyracksDataException {
        throw new HyracksDataException("Failed on hash partitioning.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#close()
     */
    @Override
    public void close() throws HyracksDataException {
        outputBuffers = null;
        for (RunFileWriter partWriter : partitions) {
            if (partWriter != null)
                partWriter.close();
        }
    }

    public void finishup() throws HyracksDataException {
        for (int i = 0; i < outputBuffers.length; i++) {
            if (outputBuffers[i] == null) {
                continue;
            }
            if (partitions[i] == null) {
                partitions[i] = new RunFileWriter(ctx.getJobletContext().createManagedWorkspaceFile(
                        HybridHashPartitionGenerateFrameWriter.class.getSimpleName()), ctx.getIOManager());
                partitions[i].open();
            }
            outFrameTupleAppender.reset(outputBuffers[i], false);
            if (outFrameTupleAppender.getTupleCount() > 0) {
                FrameUtils.flushFrame(outputBuffers[i], partitions[i]);
                partitionRawSizesInFrames[i]++;
                outputBuffers[i] = null;
            }
        }

        partitionRunReaders = new LinkedList<IFrameReader>();
        partitionSizeInFrames = new LinkedList<Integer>();
        partitionSizeInTuples = new LinkedList<Integer>();
        partitionRunAggregatedPages = new LinkedList<Integer>();

        for (int i = 0; i < numOfPartitions; i++) {
            if (partitions[i] != null) {
                partitionRunReaders.add(partitions[i].createReader());
                partitionRunAggregatedPages.add(0);
                partitions[i].close();
                partitionSizeInFrames.add(partitionRawSizesInFrames[i]);
                partitionSizeInTuples.add(partitionRawSizesInTuples[i]);
            }
        }

    }

    public List<IFrameReader> getSpilledRuns() throws HyracksDataException {
        return partitionRunReaders;
    }

    public List<Integer> getSpilledRunsSizeInPages() throws HyracksDataException {
        return partitionSizeInFrames;
    }
}
