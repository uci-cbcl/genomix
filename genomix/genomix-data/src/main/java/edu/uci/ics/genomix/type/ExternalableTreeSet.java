package edu.uci.ics.genomix.type;

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
    static private int countLimit = Integer.MAX_VALUE;

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

    protected TreeSet<T> inMemorySet;
    private Path path;
    protected boolean isChanged;

    public ExternalableTreeSet() {
        this(null);
    }

    protected ExternalableTreeSet(Path path) {
        inMemorySet = new TreeSet<T>();
        this.path = path;
        isChanged = false;
    }

    public boolean add(T t) {
        boolean contains = inMemorySet.add(t);
        if (contains) {
            isChanged = contains;
        }
        return contains;
    }

    public boolean remove(T t) {
        boolean contains = inMemorySet.remove(t);
        if (contains) {
            isChanged = contains;
        }
        return contains;
    }

    public boolean contains(T obj) {
        return inMemorySet.contains(obj);
    }

    public int size() {
        return inMemorySet.size();
    }

    /**
     * Returns a view of the portion of this set whose elements range from fromElement, inclusive, to
     * toElement, exclusive. (If fromElement and toElement are equal, the returned set is empty.)
     * [lowKey, highKey)
     * 
     * @param lowKey
     * @param highKey
     * @return
     */
    public SortedSet<T> rangeSearch(T lowKey, T highKey) {
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

    public abstract T readEachElementFromDataStream(DataInput in) throws IOException;

    public abstract void writeEachElementToDataStream(DataOutput out, T t) throws IOException;

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        inMemorySet.clear();
        path = null;
        if (size < countLimit) {
            for (int i = 0; i < size; ++i) {
                inMemorySet.add(readEachElementFromDataStream(in));
            }
        } else {
            path = new Path(in.readUTF());
            try {
                inMemorySet = (TreeSet<T>) load(path);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        isChanged = false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(inMemorySet.size());
        if (inMemorySet.size() < countLimit) {
            for (T t : inMemorySet) {
                writeEachElementToDataStream(out, t);
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
