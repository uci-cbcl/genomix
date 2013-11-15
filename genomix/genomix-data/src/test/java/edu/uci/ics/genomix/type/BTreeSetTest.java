package edu.uci.ics.genomix.type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.util.Marshal;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.pregelix.core.data.TypeTraits;

public class BTreeSetTest {
    @Test
    public void TestAll() throws IOException, IndexException {

        ISerializerDeserializer[] fields = new ISerializerDeserializer[1];
        TypeTraits[] traits = { new TypeTraits(true) };

        RecordDescriptor recordDescriptor = new RecordDescriptor(fields);

        IBinaryComparatorFactory[] factory = { new IBinaryComparatorFactory() {

            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public IBinaryComparator createBinaryComparator() {
                return new IBinaryComparator() {

                    @Override
                    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                        return Marshal.getInt(b1, s1) - Marshal.getInt(b2, s2);
                    }

                };
            }
        } };

        BTreeSet set = new BTreeSet(recordDescriptor, traits, factory);

        ArrayTupleReference tuple = new ArrayTupleReference();
        int[] fieldlengths = { 4 };
        Random random = new Random();

        ArrayList<Integer> testData = new ArrayList<Integer>();
        int count = 10;
        byte[] intBytes = new byte[4];
        for (int i = 0; i < count; i++) {
            int num = random.nextInt();
            testData.add(num);
            Marshal.putInt(num, intBytes, 0);
            tuple.reset(fieldlengths, intBytes);
            set.insert(tuple);
        }
        set.active();

        Collections.sort(testData);
        ITreeIndexCursor cursor = set.createScanCursor();
        int i = 0;
        while (cursor.hasNext()) {
            cursor.next();
            ITupleReference t = cursor.getTuple();
            Assert.assertEquals(testData.get(i++).intValue(), Marshal.getInt(t.getFieldData(0), t.getFieldStart(0)));
        }
        Assert.assertEquals(i, testData.size());
        set.deactive();
        set.destroy();
    }
}
