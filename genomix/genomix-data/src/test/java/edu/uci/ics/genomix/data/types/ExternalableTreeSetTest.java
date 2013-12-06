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

        @Override
        public TestIntWritable readEachNonGenericElement(DataInput in) throws IOException {
            TestIntWritable iw = new TestIntWritable();
            iw.readFields(in);
            return iw;
        }

        @Override
        public void writeEachNonGenericElement(DataOutput out, TestIntWritable t) throws IOException {
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
        for (TestIntWritable it : eSet.inMemorySet) {
            Assert.assertEquals(i++, it.get());
        }
    }

    @Test
    public void TestIterator() {
        ExternalableTreeSet.setCountLimit(limit);

        ExternalableTreeSet<TestIntWritable> eSet = new TestExternalableTreeSet();
        for (int i = 0; i < limit; i++) {
            eSet.add(new TestIntWritable(i));
        }

        for (TestIntWritable it : eSet.inMemorySet) {
            it.set(42);
        }

        for (TestIntWritable it : eSet.inMemorySet) {
            Assert.assertEquals(42, it.get());
        }
    }

    public void testEqual() {
        ExternalableTreeSet<TestIntWritable> eSetA = new TestExternalableTreeSet();
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
        ExternalableTreeSet<TestIntWritable> eSetB = new TestExternalableTreeSet();
        try {
            eSetB.readFields(new DataInputStream(bais));
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        Iterator<TestIntWritable> itA = eSetA.inMemorySet.iterator();
        Iterator<TestIntWritable> itB = eSetB.inMemorySet.iterator();
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

    @Test
    public void TestHDFS() {
        final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";

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
            ExternalableTreeSet.setupManager(conf, tmpPath);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getLocalizedMessage());
            return;
        }

        testEqual();
        localDFSCluster.shutdown();
    }

}
