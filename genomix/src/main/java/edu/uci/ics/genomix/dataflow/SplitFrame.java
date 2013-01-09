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
package edu.uci.ics.genomix.dataflow;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortRunGenerator;
import edu.uci.ics.hyracks.dataflow.std.structures.SerializableHashTable;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class SplitFrame {

    private static int HASH_SIZE = 4096;
    private final SerializableHashTable table;
    private final TuplePointer tempPointer;
    private ArrayTupleBuilder tupleBuilder;
    private final int buf_size;

    private final IHyracksTaskContext ctx;
    private final int[] sortFields;
    private final INormalizedKeyComputer nkc;
    private final IBinaryComparator[] comparators;
    private final ByteBuffer[] buffers;

    private final FrameTupleAccessor fta1;
    private final FrameTupleAccessor fta2;

    private final FrameTupleAppender appender;

    private final ByteBuffer outFrame;

    private int dataFrameCount;
    private int[] tPointers;
    private int tupleCount;
    private final List<IFrameReader> runs;
    private int flushCount;
    private RecordDescriptor recordDescriptor;

    private int FrameTupleCount;

    public SplitFrame(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, int buf_size) {
        this.ctx = ctx;
        this.sortFields = sortFields;
        this.recordDescriptor = recordDescriptor;

        nkc = firstKeyNormalizerFactory == null ? null : firstKeyNormalizerFactory.createNormalizedKeyComputer();
        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        fta1 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        fta2 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        appender = new FrameTupleAppender(ctx.getFrameSize());
        outFrame = ctx.allocateFrame();
        table = new SerializableHashTable(HASH_SIZE, ctx);
        dataFrameCount = 0;

        tempPointer = new TuplePointer();
        tupleBuilder = new ArrayTupleBuilder(3);
        this.buf_size = buf_size;
        buffers = new ByteBuffer[buf_size];
        for (int i = 0; i < buf_size; i++) {
            buffers[i] = ByteBuffer.allocate(ctx.getFrameSize());
        }
        appender.reset(buffers[0], true);
        flushCount = 0;
        runs = new LinkedList<IFrameReader>();
        FrameTupleCount = 0;
    }

    public void reset() {
        dataFrameCount = 0;
        tupleCount = 0;
        appender.reset(buffers[0], true);
    }

    public int getFrameCount() {
        return dataFrameCount;
    }

    private void SearchHashTable(long entry, TuplePointer dataPointer) {
        int offset = 0;
        int tp = (int) (entry % HASH_SIZE);
        if (tp < 0) {
            tp = -tp;
        }
        do {
            table.getTuplePointer(tp, offset, dataPointer);// what is the offset mean?
            if (dataPointer.frameIndex < 0 || dataPointer.tupleIndex < 0)
                break;
            int bIndex = dataPointer.frameIndex;
            int tIndex = dataPointer.tupleIndex;
            fta1.reset(buffers[bIndex]);

            /* System.out.print("a:");
             System.out.print(tIndex);
             System.out.print(" b");
             System.out.print(fta1.getTupleCount());
             System.out.println();*/

            int tupleOffset = fta1.getTupleStartOffset(tIndex);
            int fieldOffset = fta1.getFieldStartOffset(tIndex, 0);
            int slotLength = fta1.getFieldSlotsLength();
            int pos = tupleOffset + fieldOffset + slotLength;
            long l = buffers[bIndex].getLong(pos);
            if (l == entry) {
                break;
            }
            offset += 1;
        } while (true);
    }

    private void InsertHashTable(long entry, int frame_id, int tuple_id) {

        tempPointer.frameIndex = frame_id;
        tempPointer.tupleIndex = tuple_id;

        //System.out.print(frame_id);
        //System.out.print(' ');
        //System.out.println(tuple_id);

        int tp = (int) (entry % HASH_SIZE);
        if (tp < 0) {
            tp = -tp;
        }
        table.insert(tp, tempPointer);

    }

    public void insertKmer(long l, byte r) {
        try {
            SearchHashTable(l, tempPointer);
            if (tempPointer.frameIndex != -1 && tempPointer.tupleIndex != -1) {
                fta1.reset(buffers[tempPointer.frameIndex]);
                int tStart = fta1.getTupleStartOffset(tempPointer.tupleIndex);
                int f0StartRel = fta1.getFieldStartOffset(tempPointer.tupleIndex, 1);
                int slotLength = fta1.getFieldSlotsLength();
                int pos = f0StartRel + tStart + slotLength;

                buffers[tempPointer.frameIndex].array()[pos] |= r;
                buffers[tempPointer.frameIndex].position(pos + 1);
                int temp_int = buffers[tempPointer.frameIndex].getInt();
                temp_int += 1;
                buffers[tempPointer.frameIndex].position(pos + 1);
                buffers[tempPointer.frameIndex].putInt(temp_int);
            } else {
                tupleBuilder.reset();
                tupleBuilder.addField(Integer64SerializerDeserializer.INSTANCE, l);
                tupleBuilder.addField(ByteSerializerDeserializer.INSTANCE, r);
                tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, 1);

                /*System.out.print(l);
                System.out.print(' ');
                System.out.print(r);
                System.out.println();*/
                boolean b = appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                        tupleBuilder.getSize());

                if (!b) {
                    dataFrameCount++;
                    FrameTupleCount = 0;
                    if (dataFrameCount < buf_size) {
                        appender.reset(buffers[dataFrameCount], true);
                    } else {
                        sortFrames();
                        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                                ExternalSortRunGenerator.class.getSimpleName());
                        RunFileWriter writer = new RunFileWriter(file, ctx.getIOManager());
                        writer.open();
                        try {
                            flushCount += 1;
                            flushFrames(writer);
                        } finally {
                            writer.close();
                        }
                        runs.add(writer.createReader());
                        dataFrameCount = 0;
                        appender.reset(buffers[dataFrameCount], true);
                    }
                    boolean tb = appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                            tupleBuilder.getSize());
                    if (!tb) {
                        throw new HyracksDataException(
                                "Failed to copy an record into a frame: the record size is too large");
                    }
                }
                InsertHashTable(l, dataFrameCount, FrameTupleCount);
                FrameTupleCount += 1;
            }
        } catch (HyracksDataException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void sortFrames() {
        int nBuffers = dataFrameCount;
        tupleCount = 0;
        for (int i = 0; i < nBuffers; ++i) {
            fta1.reset(buffers[i]);
            tupleCount += fta1.getTupleCount();
        }
        int sfIdx = sortFields[0];
        tPointers = tPointers == null || tPointers.length < tupleCount * 4 ? new int[tupleCount * 4] : tPointers;
        int ptr = 0;
        for (int i = 0; i < nBuffers; ++i) {
            fta1.reset(buffers[i]);
            int tCount = fta1.getTupleCount();
            byte[] array = fta1.getBuffer().array();
            for (int j = 0; j < tCount; ++j) {
                int tStart = fta1.getTupleStartOffset(j);
                int tEnd = fta1.getTupleEndOffset(j);
                tPointers[ptr * 4] = i;
                tPointers[ptr * 4 + 1] = tStart;
                tPointers[ptr * 4 + 2] = tEnd;
                int f0StartRel = fta1.getFieldStartOffset(j, sfIdx);
                int f0EndRel = fta1.getFieldEndOffset(j, sfIdx);
                int f0Start = f0StartRel + tStart + fta1.getFieldSlotsLength();
                tPointers[ptr * 4 + 3] = nkc == null ? 0 : nkc.normalize(array, f0Start, f0EndRel - f0StartRel);
                ++ptr;
            }
        }
        if (tupleCount > 0) {
            sort(tPointers, 0, tupleCount);
        }

        DumpAllBuffers();
        //point the pointer to the first one
        dataFrameCount = 0;
    }

    public void flushFrames(IFrameWriter writer) throws HyracksDataException {
        appender.reset(outFrame, true);
        for (int ptr = 0; ptr < tupleCount; ++ptr) {
            int i = tPointers[ptr * 4];
            int tStart = tPointers[ptr * 4 + 1];
            int tEnd = tPointers[ptr * 4 + 2];
            ByteBuffer buffer = buffers[i];
            fta1.reset(buffer);
            if (!appender.append(fta1, tStart, tEnd)) {
                FrameUtils.flushFrame(outFrame, writer);
                appender.reset(outFrame, true);
                if (!appender.append(fta1, tStart, tEnd)) {
                    throw new IllegalStateException();
                }
            }
        }
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outFrame, writer);
        }
    }

    private void sort(int[] tPointers, int offset, int length) {
        int m = offset + (length >> 1);
        int mi = tPointers[m * 4];
        int mj = tPointers[m * 4 + 1];
        int mv = tPointers[m * 4 + 3];

        int a = offset;
        int b = a;
        int c = offset + length - 1;
        int d = c;
        while (true) {
            while (b <= c) {
                int cmp = compare(tPointers, b, mi, mj, mv);
                if (cmp > 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(tPointers, a++, b);
                }
                ++b;
            }
            while (c >= b) {
                int cmp = compare(tPointers, c, mi, mj, mv);
                if (cmp < 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(tPointers, c, d--);
                }
                --c;
            }
            if (b > c)
                break;
            swap(tPointers, b++, c--);
        }

        int s;
        int n = offset + length;
        s = Math.min(a - offset, b - a);
        vecswap(tPointers, offset, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswap(tPointers, b, n - s, s);

        if ((s = b - a) > 1) {
            sort(tPointers, offset, s);
        }
        if ((s = d - c) > 1) {
            sort(tPointers, n - s, s);
        }
    }

    private void swap(int x[], int a, int b) {
        for (int i = 0; i < 4; ++i) {
            int t = x[a * 4 + i];
            x[a * 4 + i] = x[b * 4 + i];
            x[b * 4 + i] = t;
        }
    }

    private void vecswap(int x[], int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swap(x, a, b);
        }
    }

    private int compare(int[] tPointers, int tp1, int tp2i, int tp2j, int tp2v) {
        int i1 = tPointers[tp1 * 4];
        int j1 = tPointers[tp1 * 4 + 1];
        int v1 = tPointers[tp1 * 4 + 3];
        if (v1 != tp2v) {
            return ((((long) v1) & 0xffffffffL) < (((long) tp2v) & 0xffffffffL)) ? -1 : 1;
        }
        int i2 = tp2i;
        int j2 = tp2j;
        ByteBuffer buf1 = buffers[i1];
        ByteBuffer buf2 = buffers[i2];
        byte[] b1 = buf1.array();
        byte[] b2 = buf2.array();
        fta1.reset(buf1);
        fta2.reset(buf2);
        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = sortFields[f];
            int f1Start = fIdx == 0 ? 0 : buf1.getInt(j1 + (fIdx - 1) * 4);
            int f1End = buf1.getInt(j1 + fIdx * 4);
            int s1 = j1 + fta1.getFieldSlotsLength() + f1Start;
            int l1 = f1End - f1Start;
            int f2Start = fIdx == 0 ? 0 : buf2.getInt(j2 + (fIdx - 1) * 4);
            int f2End = buf2.getInt(j2 + fIdx * 4);
            int s2 = j2 + fta2.getFieldSlotsLength() + f2Start;
            int l2 = f2End - f2Start;
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    public void close() {
        //this.buffers.clear();
    }

    public int getFlushCount() {
        return flushCount;
    }

    /*public void AddRuns(RunFileWriter r){
    	try {
    		runs.add(r.createReader());
    	} catch (HyracksDataException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    }*/

    public List<IFrameReader> GetRuns() {
        return runs;
    }

    private void DumpAllBuffers() {
        FrameTupleAccessor tfa = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);

        for (int i = 0; i < dataFrameCount; i++) {
            tfa.reset(buffers[i]);
            int t = tfa.getTupleCount();
            for (int j = 0; j < t; j++) {
                int tupleOffset = tfa.getTupleStartOffset(j);

                int r = tfa.getFieldStartOffset(j, 0);
                int pos = tupleOffset + r + tfa.getFieldSlotsLength();
                long l = buffers[i].getLong(pos);
                System.out.print(l);
                System.out.print(' ');

                r = tfa.getFieldStartOffset(j, 1);
                pos = tupleOffset + r + tfa.getFieldSlotsLength();
                byte b = buffers[i].array()[pos];
                System.out.print(b);
                System.out.print(' ');

                r = tfa.getFieldStartOffset(j, 2);
                pos = tupleOffset + r + tfa.getFieldSlotsLength();
                int o = buffers[i].getInt(pos);
                System.out.print(o);
                System.out.print(' ');

                System.out.println();
            }
        }
        System.out.println("---------------------------------");
    }

    //functions for dubugging
    //    private void DumpBuffer(byte[] f) {
    //        int n = f.length;
    //
    //        int count = 0;
    //        for (int i = 0; i < n; i++) {
    //            if (i % 13 == 0) {
    //                if (count != 0) {
    //                    System.out.print(")(");
    //                } else {
    //                    System.out.print("(");
    //                }
    //                System.out.print(count);
    //                System.out.print(':');
    //                count += 1;
    //            }
    //            System.out.print(f[i]);
    //            System.out.print(' ');
    //        }
    //        System.out.println(')');
    //    }

    public void processLastFrame() {
        sortFrames();
        FileReference file;

        DumpAllBuffers();
        try {
            file = ctx.getJobletContext().createManagedWorkspaceFile(ExternalSortRunGenerator.class.getSimpleName());
            RunFileWriter writer = new RunFileWriter(file, ctx.getIOManager());
            writer.open();
            try {
                flushCount += 1;
                flushFrames(writer);
            } finally {
                writer.close();
            }
            runs.add(writer.createReader());
        } catch (HyracksDataException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //frameSorter.AddRuns((RunFileWriter) writer);

    }
}