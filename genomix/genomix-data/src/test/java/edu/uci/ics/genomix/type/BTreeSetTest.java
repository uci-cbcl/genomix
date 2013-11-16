package edu.uci.ics.genomix.type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.util.Marshal;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.pregelix.core.data.TypeTraits;

public class BTreeSetTest {

    IBinaryComparatorFactory[] comparaterFactory = { new IBinaryComparatorFactory() {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        @Override
        public IBinaryComparator createBinaryComparator() {
            return new IBinaryComparator() {

                @Override
                public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                    int left = Marshal.getInt(b1, s1);
                    int right = Marshal.getInt(b2, s2);
                    if (left < right) {
                        return -1;
                    } else if (left > right) {
                        return 1;
                    } else {
                        return 0;
                    }
                }

            };
        }
    } };

    TypeTraits[] traits = { new TypeTraits(4) };

    RecordDescriptor recordDescriptor = new RecordDescriptor(new ISerializerDeserializer[1]);
    int[] fieldlengths = { 4 };

    @Test
    public void TestInsert() throws IOException, IndexException {

        BTreeSet set = new BTreeSet(recordDescriptor, traits, comparaterFactory);
        ArrayList<Integer> testData = createRandomTestSetAndFeedBTree(20, set);
        set.active();

        verifyBTreeData(testData, set, false);
        set.deactive();
        set.destroy();
    }

    @Test
    public void TestLoadStore() throws IOException, IndexException {
        BTreeSet setSave = new BTreeSet(recordDescriptor, traits, comparaterFactory);
        int count = 128;
        ArrayList<Integer> testData = createRandomTestSetAndFeedBTree(count, setSave);
        setSave.active();

        RunFileWriter writer = new RunFileWriter(BTreeSet.manager.newFileReference(), BTreeSet.manager.getIOManager());
        setSave.save(writer);

        BTreeSet setLoad = new BTreeSet(recordDescriptor, traits, comparaterFactory);
        setLoad.active();
        setLoad.load(count, writer.createReader());

        verifyBTreeData(testData, setLoad, false);

        setSave.deactive();
        setSave.destroy();

        setLoad.deactive();
        setLoad.destroy();
    }

    @Test
    public void TestUnion() throws IOException, IndexException {
        BTreeSet setLeft = new BTreeSet(recordDescriptor, traits, comparaterFactory);
        int count = 256;
        ArrayList<Integer> leftData = createRandomTestSetAndFeedBTree(count, setLeft);
        setLeft.active();

        BTreeSet setRight = new BTreeSet(recordDescriptor, traits, comparaterFactory);
        ArrayList<Integer> rightData = createRandomTestSetAndFeedBTree(count, setRight);
        setRight.active();

        setLeft.unionWith(setRight);

        HashSet<Integer> set = new HashSet<Integer>();
        set.addAll(leftData);
        set.addAll(rightData);
        verifyBTreeData(new ArrayList<Integer>(set), setLeft, false);
    }

    @Test
    public void TestIntersect() throws IOException, IndexException {
        BTreeSet setLeft = new BTreeSet(recordDescriptor, traits, comparaterFactory);
        int count = 256;
        ArrayList<Integer> leftData = createRandomTestSetAndFeedBTree(count, setLeft);
        setLeft.active();

        BTreeSet setRight = new BTreeSet(recordDescriptor, traits, comparaterFactory);
        ArrayTupleReference tuple = new ArrayTupleReference();
        byte[] intBytes = new byte[4];
        for (int num : leftData) {
            Marshal.putInt(num, intBytes, 0);
            tuple.reset(fieldlengths, intBytes);
            setRight.insert(tuple);
        }

        setLeft.intersectWith(setRight);

        verifyBTreeData(leftData, setLeft, false);
    }

    public ArrayList<Integer> createRandomTestSetAndFeedBTree(int count, BTreeSet btree) throws HyracksDataException,
            TreeIndexException {
        ArrayTupleReference tuple = new ArrayTupleReference();
        Random random = new Random();
        ArrayList<Integer> testData = new ArrayList<Integer>();
        byte[] intBytes = new byte[4];
        for (int i = 0; i < count; i++) {
            int num = random.nextInt();
            testData.add(num);
            Marshal.putInt(num, intBytes, 0);
            tuple.reset(fieldlengths, intBytes);
            btree.insert(tuple);
        }
        return testData;
    }

    public void verifyBTreeData(ArrayList<Integer> expected, BTreeSet btree, boolean showlog)
            throws HyracksDataException, IndexException {
        Collections.sort(expected);
        ITreeIndexCursor cursor = btree.createSortedOrderCursor();
        int i = 0;
        while (cursor.hasNext()) {
            cursor.next();
            ITupleReference t = cursor.getTuple();
            if (showlog) {
                System.out.println("expected: " + expected.get(i) + " || actual:"
                        + Marshal.getInt(t.getFieldData(0), t.getFieldStart(0)));
            }
            Assert.assertEquals(expected.get(i++).intValue(), Marshal.getInt(t.getFieldData(0), t.getFieldStart(0)));
        }
        Assert.assertEquals(i, expected.size());
        cursor.close();
    }
}
