package edu.uci.ics.genomix.dataflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class FileScanDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private int k;
    private String filename;

    public FileScanDescriptor(IOperatorDescriptorRegistry spec, int k, String filename) {
        super(spec, 0, 1);
        // TODO Auto-generated constructor stub
        this.k = k;
        this.filename = filename;
        //recordDescriptors[0] = news RecordDescriptor(
        //		new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });
        recordDescriptors[0] = new RecordDescriptor(new ISerializerDeserializer[] {
                Integer64SerializerDeserializer.INSTANCE, ByteSerializerDeserializer.INSTANCE });
    }

    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {

        final int temp = partition;

        // TODO Auto-generated method stub
        return (IOperatorNodePushable) new AbstractUnaryOutputSourceOperatorNodePushable() {
            private ArrayTupleBuilder tupleBuilder;
            private ByteBuffer outputBuffer;
            private FrameTupleAppender outputAppender;
            private long window;

            @Override
            public void initialize() {

                window = 0;
                for (int i = 0; i < k; i++) {
                    window <<= 2;
                    window |= 3;
                }

                tupleBuilder = new ArrayTupleBuilder(2);
                outputBuffer = ctx.allocateFrame();
                outputAppender = new FrameTupleAppender(ctx.getFrameSize());
                outputAppender.reset(outputBuffer, true);
                try {// one try with multiple catch?
                    writer.open();
                    String s = filename + String.valueOf(temp);
                    
                    File tf = new File(s);
                    
                    File[] fa = tf.listFiles();

                    for(int i = 0 ; i < fa.length ; i++){
                        BufferedReader readsfile = new BufferedReader(new InputStreamReader(new FileInputStream(fa[i])));
                        String read = readsfile.readLine();
                        while (read != null) {
                            read = readsfile.readLine();
                            SplitReads(read.getBytes());
                            //read.getBytes();

                            read = readsfile.readLine();
                            read = readsfile.readLine();

                            read = readsfile.readLine();
                        }
                    }
                    if (outputAppender.getTupleCount() > 0) {
                        FrameUtils.flushFrame(outputBuffer, writer);
                    }
                    outputAppender = null;
                    outputBuffer = null;
                    // sort code for external sort here?
                    writer.close();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    throw new IllegalStateException(e);
                }
            }

            private long CompressKmer(byte[] array, int start) {
                // a: 00; c: 01; G: 10; T: 11
                long l = 0;
                for (int i = start; i < start + k; i++) {
                    l <<= 2;
                    switch (array[i]) {
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

                    for (int i = 0; i < array.length - k + 1; i++) {
                        if (0 == i) {
                            l = CompressKmer(array, i);
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

                        tupleBuilder.reset();

                        tupleBuilder.addField(Integer64SerializerDeserializer.INSTANCE, l);
                        tupleBuilder.addField(ByteSerializerDeserializer.INSTANCE, r);

                        if (!outputAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                tupleBuilder.getSize())) {
                            FrameUtils.flushFrame(outputBuffer, writer);
                            outputAppender.reset(outputBuffer, true);
                            if (!outputAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(),
                                    0, tupleBuilder.getSize())) {
                                throw new IllegalStateException(
                                        "Failed to copy an record into a frame: the record size is too large.");
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        };

    }
}
