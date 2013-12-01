package edu.uci.ics.genomix.type;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeBulkLoader;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.DelayPageCleanerPolicy;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.TransientFileMapManager;

public class BTreeSet implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private BTree btree;
    private BTreeAccessor btreeAccessor;
    private FrameTupleAccessor frameTupleAccessor;
    private FrameTupleReference frameTupleReference;
    private ArrayTupleBuilder tupleBuilder;

    private MultiComparator comparator;

    private long totalTupleCount = 0;

    protected static volatile BTreeStorageManager manager;

    private BTreeStorageManager getBTreeManager() {
        if (manager == null) {
            synchronized (BTreeStorageManager.class) {
                if (manager == null) {
                    manager = new BTreeStorageManager();
                }
            }
        }
        return manager;
    }

    public static void closeBuffer() throws HyracksDataException {
        if (manager != null) {
            manager.close();
        }
    }

    public BTreeSet(RecordDescriptor recordDescriptor, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories)
            throws BTreeException, IOException {
        getBTreeManager();
        IBinaryComparator[] comparators = new IBinaryComparator[cmpFactories.length];
        for (int i = 0; i < cmpFactories.length; i++) {
            comparators[i] = cmpFactories[i].createBinaryComparator();
        }
        this.comparator = new MultiComparator(comparators);

        this.btree = BTreeUtils.createBTree(manager.getBufferCache(), manager.getFileMapProvider(), typeTraits,
                cmpFactories, BTreeLeafFrameType.REGULAR_NSM, manager.newFileReference());
        this.btree.create();
        this.btreeAccessor = (BTreeAccessor) btree.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        this.frameTupleAccessor = new FrameTupleAccessor(manager.getFrameSize(), recordDescriptor);
        this.frameTupleReference = new FrameTupleReference();
        this.tupleBuilder = new ArrayTupleBuilder(recordDescriptor.getFieldCount());

    }

    public void active() throws HyracksDataException {
        btree.activate();
    }

    /**
     * Deactive make current BTree disconnected with buffercache.
     * It will not be able to do the other operations.
     * If will wake up by calling {@code active};
     * 
     * @throws HyracksDataException
     */
    public void deactive() throws HyracksDataException {
        btree.deactivate();
    }

    public void load(long numElementsHint, IFrameReader frameReader) throws HyracksDataException, IndexException {

        totalTupleCount = 0;

        //btree.clear(); // This clear() doesn't work well, use the following 4 steps to clean it.
        btree.deactivate();
        btree.destroy();
        btree.create();
        btree.activate();
        BTreeBulkLoader loader = (BTreeBulkLoader) btree.createBulkLoader(0.9f, false, numElementsHint, false);
        frameReader.open();
        ByteBuffer buffer = manager.allocateFrame();
        while (frameReader.nextFrame(buffer)) {
            frameTupleAccessor.reset(buffer);

            int tupleCount = frameTupleAccessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                frameTupleReference.reset(frameTupleAccessor, i);
                loader.add(frameTupleReference);
            }
            totalTupleCount += tupleCount;
        }
        frameReader.close();
        loader.end();
    }

    public void save(IFrameWriter writer) throws HyracksDataException, IndexException {
        ITreeIndexCursor scanCursor = btreeAccessor.createSearchCursor(false);
        btreeAccessor.search(scanCursor, new RangePredicate(null, null, true, true, null, null));
        ByteBuffer outputBuffer = getBTreeManager().allocateFrame();
        FrameTupleAppender outputAppender = new FrameTupleAppender(getBTreeManager().getFrameSize());
        outputAppender.reset(outputBuffer, true);

        try {
            writer.open();
            while (scanCursor.hasNext()) {
                scanCursor.next();
                writeTuple(writer, scanCursor.getTuple(), outputBuffer, outputAppender);
            }
            FrameUtils.flushFrame(outputBuffer, writer);
        } finally {
            scanCursor.close();
            writer.close();
        }
    }

    public void insert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
        btreeAccessor.insert(tuple);
        totalTupleCount++;
    }

    public void update(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
        btreeAccessor.update(tuple);
    }

    public void upsert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
        btreeAccessor.upsert(tuple);
    }

    public void delete(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
        btreeAccessor.delete(tuple);
        totalTupleCount--;
    }

    public ITupleReference contains(RangePredicate searchPred) throws HyracksDataException, IndexException {
        ITreeIndexCursor cursor = btreeAccessor.createSearchCursor(false);
        btreeAccessor.search(cursor, searchPred);
        if (cursor.hasNext()) {
            cursor.next();
            return cursor.getTuple();
        }
        return null;
    }

    public void unionWith(BTreeSet rightSet) throws HyracksException, IndexException {
        if (this == rightSet) { // self union 
            return;
        }
        BTreeAccessor leftAccessor = this.btreeAccessor;
        ITreeIndexCursor leftCursor = leftAccessor.createSearchCursor(false);
        leftAccessor.search(leftCursor, new RangePredicate(null, null, true, true, null, null));

        BTreeAccessor rightAccessor = rightSet.btreeAccessor;
        ITreeIndexCursor rightCursor = rightAccessor.createSearchCursor(false);
        rightAccessor.search(rightCursor, new RangePredicate(null, null, true, true, null, null));

        RunFileWriter writer = new RunFileWriter(manager.newFileReference(), manager.getIOManager());
        writer.open();

        if (!rightCursor.hasNext()) { // right one is empty;
            return;
        }

        if (!leftCursor.hasNext()) { // myself is empty, make it point to rightSet;
            this.deactive();
            this.destroy();
            this.btree = rightSet.btree;
            this.btreeAccessor = rightSet.btreeAccessor;
            this.totalTupleCount = rightSet.totalTupleCount;
            return;
        }

        ByteBuffer outputBuffer = getBTreeManager().allocateFrame();
        FrameTupleAppender outputAppender = new FrameTupleAppender(getBTreeManager().getFrameSize());
        outputAppender.reset(outputBuffer, true);

        leftCursor.next();
        rightCursor.next();
        boolean leftFinished = false;
        boolean rightFinished = false;
        int count = 0;
        while (!leftFinished && !rightFinished) {
            int cmp = comparator.compare(leftCursor.getTuple(), rightCursor.getTuple());
            if (cmp < 0) {
                writeTuple(writer, leftCursor.getTuple(), outputBuffer, outputAppender);
                if (leftCursor.hasNext()) {
                    leftCursor.next();
                } else {
                    leftFinished = true;
                }
            } else if (cmp == 0) {
                writeTuple(writer, rightCursor.getTuple(), outputBuffer, outputAppender);
                if (leftCursor.hasNext()) {
                    leftCursor.next();
                } else {
                    leftFinished = true;
                }
                if (rightCursor.hasNext()) {
                    rightCursor.next();
                } else {
                    rightFinished = true;
                }
            } else {// (cmp > 0)
                writeTuple(writer, rightCursor.getTuple(), outputBuffer, outputAppender);
                if (rightCursor.hasNext()) {
                    rightCursor.next();
                } else {
                    rightFinished = true;
                }
            }
            count++;
        }
        while (!leftFinished) {
            writeTuple(writer, leftCursor.getTuple(), outputBuffer, outputAppender);
            count++;
            if (leftCursor.hasNext()) {
                leftCursor.next();
            } else {
                break;
            }
        }
        while (!rightFinished) {
            writeTuple(writer, rightCursor.getTuple(), outputBuffer, outputAppender);
            count++;
            if (rightCursor.hasNext()) {
                rightCursor.next();
            } else {
                break;
            }
        }
        FrameUtils.flushFrame(outputBuffer, writer);
        writer.close();

        this.load(count, writer.createReader());
        writer.getFileReference().delete();
    }

    public void intersectWith(BTreeSet otherSet) throws TreeIndexException, IndexException, HyracksException {
        if (this == otherSet) { // self union 
            return;
        }
        RunFileWriter writer = new RunFileWriter(manager.newFileReference(), manager.getIOManager());

        BTreeAccessor smallerAccessor = otherSet.totalTupleCount < this.totalTupleCount ? otherSet.btreeAccessor
                : this.btreeAccessor;
        BTreeAccessor larggerAccessor = otherSet.totalTupleCount >= this.totalTupleCount ? otherSet.btreeAccessor
                : this.btreeAccessor;
        ITreeIndexCursor scanCursor = smallerAccessor.createDiskOrderScanCursor();
        smallerAccessor.diskOrderScan(scanCursor);

        ITreeIndexCursor searchCursor = larggerAccessor.createSearchCursor(false);

        writer.open();
        long count = 0;

        ByteBuffer outputBuffer = getBTreeManager().allocateFrame();
        FrameTupleAppender outputAppender = new FrameTupleAppender(getBTreeManager().getFrameSize());
        outputAppender.reset(outputBuffer, true);

        RangePredicate searchRange = new RangePredicate();
        while (scanCursor.hasNext()) {
            scanCursor.next();
            searchRange.setLowKey(scanCursor.getTuple(), true);
            searchRange.setHighKey(scanCursor.getTuple(), true);
            searchRange.setLowKeyComparator(comparator);
            searchRange.setHighKeyComparator(comparator);

            searchCursor.reset();
            larggerAccessor.search(searchCursor, searchRange);
            if (searchCursor.hasNext()) {
                ITupleReference tuple = scanCursor.getTuple();
                writeTuple(writer, tuple, outputBuffer, outputAppender);
                count++;
            }
        }
        FrameUtils.flushFrame(outputBuffer, writer);
        writer.close();

        this.load(count, writer.createReader());
        writer.getFileReference().delete();
    }

    private void writeTuple(IFrameWriter writer, ITupleReference tuple, ByteBuffer outputBuffer,
            FrameTupleAppender outputAppender) throws HyracksDataException {
        tupleBuilder.reset();
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            tupleBuilder.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
        }
        if (!outputAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                tupleBuilder.getSize())) {
            FrameUtils.flushFrame(outputBuffer, writer);
            outputAppender.reset(outputBuffer, true);
            if (!outputAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new IllegalStateException(
                        "Failed to copy an record into a frame: the record kmerByteSize is too large.");
            }
        }
    }

    public ITreeIndexCursor createSortedOrderCursor() throws HyracksDataException, TreeIndexException {
        ITreeIndexCursor scanCursor = btreeAccessor.createSearchCursor(false);
        btreeAccessor.search(scanCursor, new RangePredicate(null, null, true, true, null, null));
        return scanCursor;
    }

    public void destroy() throws HyracksDataException {
        btree.destroy();
    }

    private static final ThreadFactory threadFactory = new ThreadFactory() {
        public Thread newThread(Runnable r) {
            return new Thread(r);
        }
    };

    protected class BTreeStorageManager {

        private static final int PAGE_SIZE = 4096;
        private static final int PAGE_NUM = 10240;
        private static final int MAX_FILE_NUM = 10;
        private static final int FRAME_SIZE = 65535;

        protected IBufferCache bufferCache;
        protected IFileMapProvider fileMapProvider;
        protected IOManager ioManager;
        protected final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");

        public BTreeStorageManager() {
        }

        public IBufferCache getBufferCache() throws HyracksException {
            if (bufferCache == null) {
                ICacheMemoryAllocator allocator = new HeapBufferAllocator();
                IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
                IFileMapProvider fileMapProvider = getFileMapProvider();
                bufferCache = new BufferCache(getIOManager(), allocator, prs, new DelayPageCleanerPolicy(1000),
                        (IFileMapManager) fileMapProvider, PAGE_SIZE, PAGE_NUM, MAX_FILE_NUM, threadFactory);
            }
            return bufferCache;
        }

        public IFileMapProvider getFileMapProvider() {
            if (fileMapProvider == null) {
                fileMapProvider = new TransientFileMapManager();
            }
            return fileMapProvider;
        }

        public IOManager getIOManager() throws HyracksException {
            if (ioManager == null) {
                List<IODeviceHandle> devices = new ArrayList<IODeviceHandle>();
                devices.add(new IODeviceHandle(new File(System.getProperty("java.io.tmpdir")), "iodev_test_wa"));
                ioManager = new IOManager(devices, Executors.newCachedThreadPool());
            }
            return ioManager;
        }

        public FileReference newFileReference() throws HyracksException {
            return new FileReference(getIOManager().getIODevices().get(0), this.getClass().getName()
                    + simpleDateFormat.format(new Date()));
        }

        public ByteBuffer allocateFrame() {
            return ByteBuffer.allocate(FRAME_SIZE);
        }

        public int getFrameSize() {
            return FRAME_SIZE;
        }

        public void close() throws HyracksDataException {
            bufferCache.close();
        }
    }

}
