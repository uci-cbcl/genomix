package edu.uci.ics.genomix.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

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

    public ExternalableTreeSet() {
        this(null);
    }

    protected ExternalableTreeSet(Path path) {
        inMemorySet = new TreeSet<T>();
        this.path = path;
        isChanged = false;
        isLoaded = false;
    }

    /**
     * A explicit load operation from path to inMemorySet.
     * Every operation that visit the inMemorySet should call this function.
     */
    @SuppressWarnings("unchecked")
    private void loadInMemorySetFromPath() {
        if (!isLoaded && path != null) {
            try {
                inMemorySet = (TreeSet<T>) load(path);
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

    protected static TreeSet<?> load(Path path) throws IOException, ClassNotFoundException {
        InputStream fis = manager.getInputStream(path);
        ObjectInputStream ois = new ObjectInputStream(fis);
        TreeSet<?> set = (TreeSet<?>) ois.readObject();
        ois.close();
        return set;
    }

    protected static void save(Path path, final TreeSet<?> set) throws IOException {
        OutputStream fos = manager.getOutputStream(path);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(set);
        oos.close();
    }

    public abstract T readNonGenericElement(DataInput in) throws IOException;

    public abstract void writeNonGenericElement(DataOutput out, T t) throws IOException;

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        inMemorySet.clear();
        path = null;
        if (size < countLimit) {
            for (int i = 0; i < size; ++i) {
                inMemorySet.add(readNonGenericElement(in));
            }
        } else {
            path = new Path(in.readUTF());
        }

        isChanged = false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(inMemorySet.size());
        if (!isLoaded && path != null) {
            out.writeUTF(path.toString());
            return;
        }
        if (inMemorySet.size() < countLimit) {
            for (T t : inMemorySet) {
                writeNonGenericElement(out, t);
            }
            if (path != null) {
                manager.deleteFile(path);
                path = null;
            }
        } else {
            if (path == null) {
                path = manager.createFile();
                save(path, inMemorySet);
            } else if (isChanged) {
                save(path, inMemorySet);
            }
            System.err.println("Write to HDFS:" + path);
            out.writeUTF(path.toString());
        }
        isChanged = false;
    }

    public void destroy() throws IOException {
        manager.deleteFile(path);
    }

    protected static class FileManager {
        private FileSystem fs;
        private Path workPath;
        private Configuration conf;
        private HashMap<Path, OutputStream> allocatedPath;

        public FileManager(Configuration conf, Path workingPath) throws IOException {
            fs = FileSystem.get(conf);
            workPath = workingPath;
            allocatedPath = new HashMap<Path, OutputStream>();
            this.conf = conf;
        }

        public void deleteAll() throws IOException {
            for (Path path : allocatedPath.keySet()) {
                deleteFile(path);
            }

        }

        public Configuration getConfiguration() {
            return conf;
        }

        protected final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");

        public synchronized Path createFile() throws IOException {
            Path path;
            do {
                path = new Path(workPath, this.getClass().getName() + simpleDateFormat.format(new Date()));
            } while (fs.exists(path));
            allocatedPath.put(path, fs.create(path, (short) 1));
            return path;
        }

        public synchronized void deleteFile(Path path) throws IOException {
            if (path != null && allocatedPath.containsKey(path)) {
                fs.delete(path, true);
                allocatedPath.remove(path);
            }
        }

        public OutputStream getOutputStream(Path path) throws IOException {
            if (!allocatedPath.containsKey(path)) {
                throw new IOException("File not exist:" + path);
            }
            return allocatedPath.get(path);
        }

        public InputStream getInputStream(Path path) throws IOException {
            //            if (!allocatedPath.containsKey(path)) {
            //                throw new IOException("File not exist:" + path);
            //            }
            return fs.open(path);
        }
    }

}
