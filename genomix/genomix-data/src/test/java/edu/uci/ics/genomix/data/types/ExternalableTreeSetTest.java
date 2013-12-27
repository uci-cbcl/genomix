package edu.uci.ics.genomix.data.types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

public class ExternalableTreeSetTest implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    final int limit = 100;

    public class TestIntWritable implements WritableComparable<TestIntWritable>, Serializable {

        private int iw;

        public TestIntWritable() {
        }

        public TestIntWritable(int v) {
            iw = v;
        }

        public int get() {
            return iw;
        }

        public void set(int i) {
            iw = i;
        }

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        @Override
        public void readFields(DataInput in) throws IOException {
            iw = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(iw);
        }

        @Override
        public int compareTo(TestIntWritable o) {
            return Integer.compare(iw, o.iw);
        }

    }

    public class TestExternalableTreeSet extends ExternalableTreeSet<TestIntWritable> {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        public TestExternalableTreeSet() {
            super();
        }

        public TestExternalableTreeSet(boolean local) {
            super(local);
        }

        @Override
        public TestIntWritable readNonGenericElement(DataInput in) throws IOException {
            TestIntWritable iw = new TestIntWritable();
            iw.readFields(in);
            return iw;
        }

        @Override
        public void writeNonGenericElement(DataOutput out, TestIntWritable t) throws IOException {
            t.write(out);
        }

    }

    @Test
    public void TestInternal() {

        ExternalableTreeSet.setCountLimit(limit);

        ExternalableTreeSet<TestIntWritable> eSet = new TestExternalableTreeSet();
        for (int i = 0; i < limit; i++) {
            eSet.add(new TestIntWritable(i));
        }

        int i = 0;
        Iterator<TestIntWritable> it = eSet.readOnlyIterator();
        while (it.hasNext()) {
            Assert.assertEquals(i++, it.next().get());
        }
    }

    @Test
    public void TestIterator() {
        ExternalableTreeSet.setCountLimit(limit);

        ExternalableTreeSet<TestIntWritable> eSet = new TestExternalableTreeSet();
        for (int i = 0; i < limit; i++) {
            eSet.add(new TestIntWritable(i));
        }

        Iterator<TestIntWritable> it = eSet.resetableIterator();
        while (it.hasNext()) {
            it.next().set(42);
        }

        Iterator<TestIntWritable> it2 = eSet.readOnlyIterator();
        while (it2.hasNext()) {
            Assert.assertEquals(42, it2.next().get());
        }
    }

    public void testEqual(boolean local, boolean entire) {
        ExternalableTreeSet<TestIntWritable> eSetA = new TestExternalableTreeSet(local);
        for (int i = 0; i <= limit; i++) {
            eSetA.add(new TestIntWritable(i));
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream w = new DataOutputStream(baos);
        try {
            ExternalableTreeSet.forceWriteEntireBody(entire);
            eSetA.write(w);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ExternalableTreeSet<TestIntWritable> eSetB = new TestExternalableTreeSet(local);
        try {
            eSetB.readFields(new DataInputStream(bais));
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        Iterator<TestIntWritable> itA = eSetA.readOnlyIterator();
        Iterator<TestIntWritable> itB = eSetB.readOnlyIterator();
        for (int i = 0; i <= limit; i++) {
            Assert.assertEquals(itA.next().get(), itB.next().get());
        }

        try {
            ExternalableTreeSet.removeAllExternalFiles();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";

    @Test
    public void TestHDFS() {

        Configuration conf = new Configuration();
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        MiniDFSCluster localDFSCluster;
        try {
            localDFSCluster = new MiniDFSCluster(conf, 2, true, null);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
            return;
        }

        ExternalableTreeSet.setCountLimit(limit);
        Path tmpPath = new Path(conf.get("hadoop.tmp.dir", "/tmp"));
        FileSystem dfs;
        try {
            dfs = FileSystem.get(conf);
        } catch (IOException e2) {
            e2.printStackTrace();
            Assert.fail(e2.getLocalizedMessage());
            return;
        }
        try {
            dfs.mkdirs(tmpPath);
        } catch (IOException e1) {
            e1.printStackTrace();
            Assert.fail(e1.getLocalizedMessage());
        }

        try {
            ExternalableTreeSet.manager = null;
            ExternalableTreeSet.setupManager(conf, tmpPath);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getLocalizedMessage());
            return;
        }

        testEqual(false, false);
        TestWriteHDFSReadAndWriteToLocal();
        localDFSCluster.shutdown();
    }

    @Test
    public void TestLocal() {

        Configuration conf = new Configuration();
        ExternalableTreeSet.setCountLimit(limit);

        try {
            ExternalableTreeSet.manager = null;
            ExternalableTreeSet.setupManager(conf, null);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getLocalizedMessage());
            return;
        }

        testEqual(true, false);
    }

    @Test
    public void TestWriteEntireBody() {
        Configuration conf = new Configuration();
        ExternalableTreeSet.setCountLimit(limit);

        try {
            ExternalableTreeSet.manager = null;
            ExternalableTreeSet.setupManager(conf, null);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getLocalizedMessage());
            return;
        }

        testEqual(true, true);
    }

    public void TestWriteHDFSReadAndWriteToLocal() {

        ExternalableTreeSet<TestIntWritable> eSetA = new TestExternalableTreeSet(false);
        for (int i = 0; i <= limit; i++) {
            eSetA.add(new TestIntWritable(i));
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream w = new DataOutputStream(baos);
        try {
            eSetA.write(w);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ExternalableTreeSet<TestIntWritable> eSetB = new TestExternalableTreeSet(true);
        try {
            eSetB.readFields(new DataInputStream(bais));
            eSetB.write(w);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        bais = new ByteArrayInputStream(baos.toByteArray());
        ExternalableTreeSet<TestIntWritable> eSetC = new TestExternalableTreeSet(true);
        try {
            eSetC.readFields(new DataInputStream(bais));
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        Iterator<TestIntWritable> itA = eSetA.readOnlyIterator();
        Iterator<TestIntWritable> itB = eSetB.readOnlyIterator();
        Iterator<TestIntWritable> itC = eSetC.readOnlyIterator();
        for (int i = 0; i <= limit; i++) {
            int a = itA.next().get();
            Assert.assertEquals(a, itB.next().get());
            Assert.assertEquals(a, itC.next().get());
        }

        try {
            ExternalableTreeSet.removeAllExternalFiles();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void TestWriteManyTimes() {

        Configuration conf = new Configuration();
        ExternalableTreeSet.setCountLimit(limit);

        try {
            ExternalableTreeSet.manager = null;
            ExternalableTreeSet.setupManager(conf, null);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getLocalizedMessage());
            return;
        }

        ExternalableTreeSet.forceWriteEntireBody(false);
        ExternalableTreeSet<TestIntWritable> eSetA = new TestExternalableTreeSet(true);
        for (int i = 0; i <= limit; i++) {
            eSetA.add(new TestIntWritable(i));
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream w = new DataOutputStream(baos);
        try {
            eSetA.write(w);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ExternalableTreeSet<TestIntWritable> eSetB = new TestExternalableTreeSet(true);
        try {
            eSetB.readFields(new DataInputStream(bais));
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        Iterator<TestIntWritable> itA = eSetA.readOnlyIterator();
        Iterator<TestIntWritable> itB = eSetB.readOnlyIterator();
        for (int i = 0; i <= limit; i++) {
            Assert.assertEquals(itA.next().get(), itB.next().get());
        }

        int rand = RandomTestHelper.genRandomInt(100, 10000);
        eSetB.add(new TestIntWritable(rand));
        ByteArrayOutputStream baosB = new ByteArrayOutputStream();
        DataOutputStream wB = new DataOutputStream(baosB);
        try {
            eSetB.write(wB);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        ByteArrayInputStream baisB = new ByteArrayInputStream(baosB.toByteArray());
        ExternalableTreeSet<TestIntWritable> eSetC = new TestExternalableTreeSet(true);
        try {
            eSetC.readFields(new DataInputStream(baisB));
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        itB = eSetB.readOnlyIterator();
        Iterator<TestIntWritable> itC = eSetC.readOnlyIterator();
        boolean contains = false;
        for (int i = 0; i <= limit + 1; i++) {
            TestIntWritable it = itC.next();
            if (it.get() == rand) {
                contains = true;
            }
            Assert.assertEquals(itB.next().get(), it.get());
        }

        Assert.assertTrue(contains);

        try {
            ExternalableTreeSet.removeAllExternalFiles();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
