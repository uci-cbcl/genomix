/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;

public class ExternalHashGrouperAggregate {

    private static final Logger LOGGER = Logger.getLogger(ExternalHashGrouperAggregate.class.getName());

    private final int[] keyFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    private final IAggregatorDescriptorFactory aggregatorFactory;

    private final int framesLimit;
    private final ISpillableTableFactory spillableTableFactory;
    private final boolean isOutputSorted;

    private final RecordDescriptor inRecDesc, outRecDesc;

    private final IHyracksTaskContext ctx;

    private final boolean skipSort;

    private final boolean skipRun;

    private final int tableSize;

    /*
     * For aggregate phase
     */

    LinkedList<RunFileReader> runs;
    ISpillableTable gTable;
    FrameTupleAccessor inFrameAccessor;

    /*
     * For instrumenting
     */
    long ioCounter = 0;
    long sortTimeCounter = 0;
    long aggTimeStart = 0;
    long frameFlushRequest = 0;
    long frameProcessingTime = 0;
    long frameFlushTime = 0;

    /*
     * For output
     */
    private final IFrameWriter finalWriter;

    /**
     * @param ctx
     * @param keyFields
     * @param framesLimit
     * @param tableSize
     * @param comparatorFactories
     * @param firstNormalizerFactory
     * @param aggregatorFactory
     * @param mergerFactory
     * @param inRecDesc
     * @param outRecDesc
     * @param spillableTableFactory
     * @param isOutputSorted
     */
    public ExternalHashGrouperAggregate(IHyracksTaskContext ctx, int[] keyFields, int framesLimit, int tableSize,
            IBinaryComparatorFactory[] comparatorFactories, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc,
            ISpillableTableFactory spillableTableFactory, IFrameWriter finalWriter, boolean isOutputSorted) {
        this.keyFields = keyFields;
        this.comparatorFactories = comparatorFactories;
        this.firstNormalizerFactory = firstNormalizerFactory;
        this.aggregatorFactory = aggregatorFactory;
        this.framesLimit = framesLimit;
        this.spillableTableFactory = spillableTableFactory;
        this.isOutputSorted = isOutputSorted;
        this.tableSize = tableSize;
        this.inRecDesc = inRecDesc;
        this.outRecDesc = outRecDesc;
        this.ctx = ctx;
        this.finalWriter = finalWriter;

        this.skipSort = false;
        this.skipRun = false;
    }

    public void initGrouperForAggregate() throws HyracksDataException {
        runs = new LinkedList<RunFileReader>();
        gTable = spillableTableFactory.buildSpillableTable(ctx, keyFields, comparatorFactories, firstNormalizerFactory,
                aggregatorFactory, inRecDesc, outRecDesc, tableSize, framesLimit);
        gTable.reset();
        inFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);
    }

    public void insertFrame(ByteBuffer buffer) throws HyracksDataException {
        inFrameAccessor.reset(buffer);
        int tupleCount = inFrameAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            if (!gTable.insert(inFrameAccessor, i)) {
                flushFramesToRun();
            }
        }
    }

    private void flushFramesToRun() throws HyracksDataException {
        frameFlushRequest++;
        long frameFlushTimer = System.currentTimeMillis();

        if (!skipSort) {
            long sortStart = System.currentTimeMillis();
            gTable.sortFrames();
            sortTimeCounter += System.currentTimeMillis() - sortStart;
        }

        if (!skipRun) {
            FileReference runFile;
            try {
                runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                        ExternalGroupOperatorDescriptor.class.getSimpleName());
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            RunFileWriter writer = new RunFileWriter(runFile, ctx.getIOManager());
            writer.open();
            try {
                gTable.flushFrames(writer, true);
            } catch (Exception ex) {
                throw new HyracksDataException(ex);
            } finally {
                writer.close();
                ioCounter += writer.getFileSize();
            }
            runs.add(((RunFileWriter) writer).createReader());
        }
        gTable.reset();
        frameFlushTime = System.currentTimeMillis() - frameFlushTimer;
    }

    public void finishAggregation() throws HyracksDataException {
        gTable.finishup(isOutputSorted);
        if (gTable.getFrameCount() >= 0 && runs.size() > 0) {
            flushFramesToRun();
            gTable.close();
            gTable = null;
        } else if (gTable.getFrameCount() >= 0 && runs.size() <= 0) {
            // Write the output
            gTable.flushFrames(finalWriter, false);
        }
        LOGGER.warning("[C]Hybrid AggregateActivity - RunIO    " + ioCounter);
        LOGGER.warning("[C]Hybrid AggregateActivity - FlushFrames    " + frameFlushRequest);
        LOGGER.warning("[T]Hybrid AggregateActivity - Sort    " + sortTimeCounter);
        LOGGER.warning("[T]Hybrid AggregateActivity - ProcessFrames    " + frameProcessingTime);
        LOGGER.warning("[T]Hybrid AggregateActivity - FlushFrames    " + frameFlushTime);
        LOGGER.warning("[T]Hybrid AggregateActivity    " + (System.currentTimeMillis() - aggTimeStart));
    }

    public LinkedList<RunFileReader> getRunReaders() {
        return runs;
    }

    public ISpillableTable getGroupTable() {
        return gTable;
    }

}
