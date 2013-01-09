package edu.uci.ics.genomix.dataflow;

import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.genomix.data.normalizers.Integer64NormalizedKeyComputerFactory;
import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
import edu.uci.ics.genomix.dataflow.GenKmerDescriptor.SplitTaskState;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class KmerSplitOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {

    private final int k;
    private long window;

    private final SplitFrame frameSorter;

    private FrameTupleAccessor accessor;
    private ArrayTupleBuilder tupleBuilder;
    private TaskId MytaskId;
    private IHyracksTaskContext ctx;

    public KmerSplitOperatorNodePushable(IHyracksTaskContext ctx, int k, RecordDescriptor rd_in, int buffer_size,
            TaskId taskid) {

        tupleBuilder = new ArrayTupleBuilder(3);
        this.k = k;

        RecordDescriptor rd = new RecordDescriptor(new ISerializerDeserializer[] {
                Integer64SerializerDeserializer.INSTANCE, ByteSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE });

        int[] sortFields = { 0 };
        frameSorter = new SplitFrame(ctx, sortFields, new Integer64NormalizedKeyComputerFactory(),
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(LongPointable.FACTORY) }, rd,
                buffer_size);

        accessor = new FrameTupleAccessor(ctx.getFrameSize(), rd_in);

        new FrameTupleAccessor(ctx.getFrameSize(), rd);
        new FrameTupleAppender(ctx.getFrameSize());

        ByteBuffer.allocate(ctx.getFrameSize());

        //initialize the window
        window = 0;
        for (int i = 0; i < k; i++) {
            window <<= 2;
            window |= 3;
        }

        MytaskId = taskid;
        this.ctx = ctx;
    }

    @Override
    public void open() throws HyracksDataException {
        // TODO Auto-generated method stub
        //writer.open();

    }

    @Override
    public void nextFrame(ByteBuffer buffer) {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        ByteBuffer temp_buf = accessor.getBuffer();
        for (int i = 0; i < tupleCount; i++) {
            int tupleStartOffset = accessor.getTupleStartOffset(i);
            int fieldStartOffset = accessor.getFieldStartOffset(i, 0);
            int loadLength = accessor.getFieldLength(i, 0);
            //int loadLength = temp_buf.getInt(tupleStartOffset);
            byte[] read = new byte[loadLength];
            int slotLength = accessor.getFieldSlotsLength();
            //temp_buf.position(tupleStartOffset+fieldStartOffset + accessor.getFieldSlotsLength());
            int pos = tupleStartOffset + fieldStartOffset + slotLength;
            //temp_buf
            try {
                temp_buf.position(pos);
                temp_buf.get(read, 0, loadLength);
                SplitReads(read);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            tupleBuilder.reset();

        }
    }

    private long CompressKmer(byte[] array, int start, int k) {
        // a: 00; c: 01; G: 10; T: 11
        long l = 0;
        for (int i = start; i < start + k; i++) {
            l <<= 2;
            switch (array[start + i]) {
                case 'A':
                case 'a':
                    l |= 0;
                    break;
                case 'C':
                case 'c':
                    l |= 1;
                    break;
                case 'G':
                case 'g':
                    l |= 2;
                    break;
                case 'T':
                case 't':
                    l |= 3;
                    break;
            }
        }
        return l;
    }

    private byte GetBitmap(byte t) {
        byte r = 0;
        switch (t) {
            case 'A':
            case 'a':
                r = 1;
                break;
            case 'C':
            case 'c':
                r = 2;
                break;
            case 'G':
            case 'g':
                r = 4;
                break;
            case 'T':
            case 't':
                r = 8;
                break;
        }
        return r;
    }

    private byte ConvertSymbol(byte t) {
        byte r = 0;
        switch (t) {
            case 'A':
            case 'a':
                r = 0;
                break;
            case 'C':
            case 'c':
                r = 1;
                break;
            case 'G':
            case 'g':
                r = 2;
                break;
            case 'T':
            case 't':
                r = 3;
                break;
        }
        return r;
    }

    private void SplitReads(byte[] array) {
        try {
            long l = 0;

            byte pre = 0, next = 0;
            byte r;

            for (int i = 2; i < array.length - k + 1; i++) {
                if (2 == i) {
                    l = CompressKmer(array, i, k);
                } else {
                    l <<= 2;
                    l &= window;
                    l |= ConvertSymbol(array[i + k - 1]);
                    pre = GetBitmap(array[i - 1]);
                }
                if (i + k != array.length) {
                    next = GetBitmap(array[i + k]);
                }

                r = 0;
                r |= pre;
                r <<= 4;
                r |= next;

                /*System.out.print(l);
                System.out.print(' ');
                System.out.print(r);
                System.out.println();*/

                frameSorter.insertKmer(l, r);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        try {
            frameSorter.processLastFrame();
            SplitTaskState state = new SplitTaskState(ctx.getJobletContext().getJobId(), MytaskId,
                    frameSorter.GetRuns());
            ctx.setStateObject(state);

            //writer.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public List<IFrameReader> GetRuns() {
        return frameSorter.GetRuns();
    }

    //for debug
    /*	private void DumpBlock(ByteBuffer f){
    		
    		int n = f.array().length/13;
    		
    		for(int i = 0 ; i < n ; i++){
    			long t = LongPointable.getLong(f.array(), 13 * i);
    			System.out.print(t);
    			System.out.print(' ');
    		}
    		System.out.println();
    	}*/
}
