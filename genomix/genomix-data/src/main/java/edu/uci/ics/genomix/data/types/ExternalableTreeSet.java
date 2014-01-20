package edu.uci.ics.genomix.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.pregelix.api.util.BspUtils;

public abstract class ExternalableTreeSet<T extends WritableComparable<T> & Serializable> implements Writable,
        Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    static FileManager manager;
    //    static private int countLimit = Integer.MAX_VALUE;
    static private int countLimit = 1000;

    public static synchronized void setupManager(Configuration conf, Path workPath) throws IOException {
        if (manager == null) {
            manager = new FileManager(conf, workPath);
        }
    }

    public static synchronized void removeAllExternalFiles() throws IOException {
        if (manager != null) {
            manager.deleteAll();
        }
    }

    public static void setCountLimit(int count) {
        countLimit = count;
    }

    private TreeSet<T> inMemorySet;
    protected Path path;
    protected boolean isChanged;
    protected boolean isLoaded;
    protected boolean writeToLocal;
    protected boolean readFromLocal;
    private static boolean writeEntireBody;

    public ExternalableTreeSet() {
        this(false);
    }

    public ExternalableTreeSet(boolean writeToLocal) {
        this(null, writeToLocal);
    }

    protected ExternalableTreeSet(Path path, boolean writeTolocal) {
        inMemorySet = new TreeSet<T>();
        this.path = path;
        isChanged = false;
        isLoaded = false;
        this.writeToLocal = writeTolocal;
        this.readFromLocal = this.writeToLocal;
    }

    public void reset() {
        loadInMemorySetFromPath();
        if (inMemorySet.size() > 0) {
            isChanged = true;
        }
        inMemorySet.clear();
    }

    /**
     * A explicit load operation from path to inMemorySet.
     * Every operation that visit the inMemorySet should call this function.
     */
    @SuppressWarnings("unchecked")
    private void loadInMemorySetFromPath() {
        if (!isLoaded && path != null) {
            try {
                inMemorySet = (TreeSet<T>) load(path, readFromLocal);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            isLoaded = true;
        }
    }

    public boolean add(T t) {
        loadInMemorySetFromPath();
        boolean contains = inMemorySet.add(t);
        if (contains) {
            isChanged = contains;
        }
        return contains;
    }

    public boolean remove(T t) {
        loadInMemorySetFromPath();
        boolean contains = inMemorySet.remove(t);
        if (contains) {
            isChanged = contains;
        }
        return contains;
    }

    public boolean contains(T obj) {
        loadInMemorySetFromPath();
        return inMemorySet.contains(obj);
    }

    public int size() {
        loadInMemorySetFromPath();
        return inMemorySet.size();
    }

    /**
     * Returns a view of the portion of this set whose elements range from fromElement, inclusive, to
     * toElement, exclusive. (If fromElement and toElement are equal, the returned set is empty.)
     * [lowKey, highKey)
     * The returned set is backed by this set, so changes in the returned set are reflected in this
     * set, and vice-versa. The returned set supports all optional set operations that this set supports.
     * The returned set will throw an IllegalArgumentException on an attempt to insert an element outside its
     * range.
     * 
     * @param lowKey
     * @param highKey
     * @return
     */
    public SortedSet<T> rangeSearch(T lowKey, T highKey) {
        loadInMemorySetFromPath();
        SortedSet<T> set = inMemorySet.subSet(lowKey, highKey);
        return set;
    }

    /**
     * Union with setB, make sure setB have already loaded before use.
     * 
     * @return true if the set changed.
     * @param setB
     */
    public boolean union(ExternalableTreeSet<T> setB) {
        loadInMemorySetFromPath();
        boolean changed = inMemorySet.addAll(setB.inMemorySet);
        if (changed) {
            isChanged = true;
        }
        return changed;
    }

    public void setAsCopy(ExternalableTreeSet<T> readSet) {
        this.inMemorySet.clear();
        this.inMemorySet.addAll(readSet.inMemorySet);
        this.path = readSet.path;
        this.isChanged = readSet.isChanged;
        if (inMemorySet.size() > 0) {
            this.isLoaded = true;
        }
    }

    protected Iterator<T> resetableIterator() {
        loadInMemorySetFromPath();
        isChanged = true;
        return inMemorySet.iterator();
    }

    protected class ReadIterator implements Iterator<T> {
        private Iterator<T> iter;

        public ReadIterator(Iterator<T> iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public T next() {
            return iter.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * You can actually still call the setting functions of T returned by this iterator,
     * But we assume the client should not call those functions.
     * 
     * @return
     */
    protected ReadIterator readOnlyIterator() {
        loadInMemorySetFromPath();
        return new ReadIterator(inMemorySet.iterator());
    }

    protected static TreeSet<?> load(Path path, boolean readFromLocal) throws IOException, ClassNotFoundException {
        InputStream fis = manager.getInputStream(path, readFromLocal);
        ObjectInputStream ois = new ObjectInputStream(fis);
        TreeSet<?> set = (TreeSet<?>) ois.readObject();
        ois.close();
        return set;
    }

    protected static void save(Path path, final TreeSet<?> set, boolean writeToLocal) throws IOException {
        OutputStream fos = manager.getOutputStream(path, writeToLocal);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(set);
        oos.close();
    }

    public abstract T readNonGenericElement(DataInput in) throws IOException;

    public abstract void writeNonGenericElement(DataOutput out, T t) throws IOException;

    @Override
    public void readFields(DataInput in) throws IOException {
        inMemorySet.clear();
        path = null;
        boolean wholeBodyInStream = in.readBoolean();
        if (wholeBodyInStream) {
            int size = in.readInt();
            for (int i = 0; i < size; ++i) {
                inMemorySet.add(readNonGenericElement(in));
            }
        } else {
            readFromLocal = in.readBoolean();
            path = new Path(in.readUTF());
        }

        isChanged = false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        boolean pathsUnmodified = !writeEntireBody && !isChanged && path != null && readFromLocal == writeToLocal;
        boolean wholeBodyInStream = writeEntireBody || inMemorySet.size() < countLimit;
        out.writeBoolean(wholeBodyInStream);

        if (pathsUnmodified) {
            out.writeBoolean(writeToLocal);
            out.writeUTF(path.toString());
            return;
        }
        if (wholeBodyInStream) {
            loadInMemorySetFromPath();
            out.writeInt(inMemorySet.size());
            for (T t : inMemorySet) {
                writeNonGenericElement(out, t);
            }
            if (path != null) {
                manager.deleteFile(path, writeToLocal);
                path = null;
            }
        } else {
            if (path == null) {
                path = manager.createFile(writeToLocal);
                save(path, inMemorySet, writeToLocal);
            } else if (isChanged) {
                save(path, inMemorySet, writeToLocal);
            }
            out.writeBoolean(writeToLocal);
            out.writeUTF(path.toString());
        }
        isChanged = false;
    }

    public void destroy() throws IOException {
        manager.deleteFile(path, writeToLocal);
    }

    protected static class FileManager {
        private FileSystem hfs;
        private FileSystem lfs;
        private Path hdfsWorkPath;
        private Path localWorkPath;
        private Configuration conf;
        private HashSet<Path> allocatedHdfsPath;
        private HashSet<Path> allocatedLocalPath;

        public FileManager(Configuration conf, Path hdfsWorkingPath) throws IOException {
            hfs = FileSystem.get(conf);
            lfs = FileSystem.getLocal(conf);
            hdfsWorkPath = hdfsWorkingPath;
            localWorkPath = new Path(BspUtils.TMP_DIR);
            allocatedHdfsPath = new HashSet<Path>();
            allocatedLocalPath = new HashSet<Path>();
            this.conf = conf;
        }

        public void deleteAll() throws IOException {
            for (Path path : allocatedHdfsPath) {
                deleteFile(path, false);
            }
            allocatedHdfsPath.clear();
            for (Path path : allocatedLocalPath) {
                deleteFile(path, true);
            }
            allocatedLocalPath.clear();
        }

        public Configuration getConfiguration() {
            return conf;
        }

        public Path createFile(boolean local) throws IOException {
            if (local) {
                return createOneFile(allocatedLocalPath, lfs, localWorkPath);
            } else {
                return createOneFile(allocatedHdfsPath, hfs, hdfsWorkPath);
            }
        }

        private static synchronized Path createOneFile(HashSet<Path> validation, FileSystem fs, Path workPath)
                throws IOException {
            Path path;
            do {
                path = new Path(workPath, "ExternalableTreeSet" + UUID.randomUUID());
            } while (fs.exists(path));
            FSDataOutputStream os = fs.create(path, (short) 1);
            os.close();
            validation.add(path);
            return path;
        }

        private static synchronized void deleteOneFile(Path path, final HashSet<Path> validation, FileSystem fs)
                throws IOException {
            if (path != null && validation.contains(path)) {
                fs.delete(path, true);
            }
        }

        public void deleteFile(Path path, boolean local) throws IOException {
            if (local) {
                deleteOneFile(path, allocatedLocalPath, lfs);
            } else {
                deleteOneFile(path, allocatedHdfsPath, hfs);
            }
        }

        private static OutputStream getOutputStream(Path path, HashSet<Path> validation, FileSystem fs)
                throws IOException {
            if (!validation.contains(path)) {
                throw new IOException("File not registered:" + path);
            }
            return fs.create(path, (short) 1);
        }

        public OutputStream getOutputStream(Path path, boolean local) throws IOException {
            if (local) {
                return getOutputStream(path, allocatedLocalPath, lfs);
            } else {
                return getOutputStream(path, allocatedHdfsPath, hfs);
            }
        }

        public InputStream getInputStream(Path path, boolean local) throws IOException {
            if (local) {
                return lfs.open(path);
            } else {
                return hfs.open(path);
            }
        }
    }

    public static void forceWriteEntireBody(boolean entire) {
        writeEntireBody = entire;
    }

}
