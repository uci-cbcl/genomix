package edu.uci.ics.genomix.dataflow;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParserFactory;

public class ReadsKeyValueParserFactory implements IKeyValueParserFactory<LongWritable, Text> {
    private static final long serialVersionUID = 1L;

    private int k;
    private int byteNum;
    
    public ReadsKeyValueParserFactory(int k){
    	this.k = k;
        byteNum = (byte)Math.ceil((double)k/4.0);
    }
    @Override
    public IKeyValueParser<LongWritable, Text> createKeyValueParser(final IHyracksTaskContext ctx) {
;
        
        final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        final ByteBuffer  outputBuffer = ctx.allocateFrame();
        final FrameTupleAppender outputAppender = new FrameTupleAppender(ctx.getFrameSize());
        outputAppender.reset(outputBuffer, true);
    	

        return new IKeyValueParser<LongWritable, Text>() {

            @Override
            public void parse(LongWritable key, Text value, IFrameWriter writer) throws HyracksDataException {
                String geneLine = value.toString(); // Read the Real Gene Line
                Pattern genePattern = Pattern.compile("[AGCT]+");
                Matcher geneMatcher = genePattern.matcher(geneLine);
                boolean isValid = geneMatcher.matches();
                if(isValid){
                	SplitReads(geneLine.getBytes(), writer);
                }
            }

            @Override
            public void flush(IFrameWriter writer) throws HyracksDataException {
                FrameUtils.flushFrame(outputBuffer, writer);
            }
            
            
            private byte[] CompressKmer(byte[] array, int start) {
                // a: 00; c: 01; G: 10; T: 11
            	
            	byte[] bytes = new byte[byteNum+1];
            	bytes[0] = (byte) k;
            	
            	byte l = 0;
            	int count = 0;
            	int bcount = 0;
            	
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
                    count += 2;
                    if(count%8==0){
                    	bcount += 1;
                    	bytes[bcount] = l;
                    	count = 0;
                    }
                }
                bytes[bcount + 1] = l;
                return bytes;
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
            
            void MoveKmer(byte[] bytes, byte c){            	
            	byte filter0 = (byte) 0xC0;
            	byte filter1 = (byte) 0xFC;
            	byte filter2 = 0;
            	
            	int r = byteNum*8 - 2*k;
            	r = 8 - r;
            	for(int i = 0 ; i < r ; i++){
            		filter2 <<= 1;
            		filter2 |= 1;
            	}

                int i = byteNum;
                bytes[i] <<= 2;
                bytes[i] &= filter2;
                i -= 1;
            	while(i > 0){
            		byte f = (byte) (bytes[i] & filter0);
            		f >>= 6;
                	bytes[i+1] |= f;
            		bytes[i] <<= 2;
            		bytes[i] &= filter1;
            	}
            	bytes[i+1] |= ConvertSymbol(c);
            }

            private void SplitReads(byte[] array, IFrameWriter writer) {
                try {
                	byte[] bytes=null;
                	
                	byte pre = 0, next = 0;
                    byte r;

                    for (int i = 0; i < array.length - k + 1; i++) {
                        if (0 == i) {
                            bytes = CompressKmer(array, i);
                        } else {
                        	MoveKmer(bytes, array[i + k - 1]);
                            /*l <<= 2;
                            l &= window;
                            l |= ConvertSymbol(array[i + k - 1]);*/
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

                        //tupleBuilder.addField(Integer64SerializerDeserializer.INSTANCE, l);
                        tupleBuilder.addField(bytes, 0, byteNum + 1);
                        tupleBuilder.addField(ByteSerializerDeserializer.INSTANCE, r);
                        
                        
                        //int[] a = tupleBuilder.getFieldEndOffsets();
                        //int b = tupleBuilder.getSize();
                        //byte[] c = tupleBuilder.getByteArray();

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