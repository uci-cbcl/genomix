package edu.uci.ics.genomix.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public abstract class ExternalableTreeSet<T extends WritableComparable<T> & Serializable> implements Writable,
        Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    static private int countLimit = 1000;

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
        InputStream fis = FileManager.getManager().getInputStream(path, readFromLocal);
        ObjectInputStream ois = new ObjectInputStream(fis);
        TreeSet<?> set = (TreeSet<?>) ois.readObject();
        ois.close();
        return set;
    }

    protected static void save(Path path, final TreeSet<?> set, boolean writeToLocal) throws IOException {
        OutputStream fos = FileManager.getManager().getOutputStream(path, writeToLocal);
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
                FileManager.getManager().deleteFile(path, writeToLocal);
                path = null;
            }
        } else {
            if (path == null) {
                path = FileManager.getManager().createDistinctFile(writeToLocal);
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
        FileManager.getManager().deleteFile(path, writeToLocal);
    }

    public static void forceWriteEntireBody(boolean entire) {
        writeEntireBody = entire;
    }

}
