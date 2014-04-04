package edu.uci.ics.genomix.data.types;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.pregelix.api.util.BspUtils;

public class FileManager {
    private FileSystem hfs;
    private FileSystem lfs;
    private Path hdfsWorkPath;
    private Path localWorkPath;
    private Configuration conf;
    private HashSet<Path> allocatedHdfsPath;
    private HashSet<Path> allocatedLocalPath;
    
    // Currently we didn't expect the different configuration pass in at the same job, 
    // So we use singleton instance and initialize it at first time. 
    private static FileManager fpManager = null;

    public static FileManager getManager() {
        if (fpManager == null) {
            synchronized (FileManager.class) {
                if (fpManager == null) {
                    fpManager = new FileManager();
                }
            }
        }
        return fpManager;
    }

    protected FileManager() {
    }

    public void initialize(Configuration conf, Path hdfsWorkingPath) throws IOException {
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

    public Path createDistinctFile(boolean local) throws IOException {
        if (local) {
            return createOneRandomFile(allocatedLocalPath, lfs, localWorkPath);
        } else {
            return createOneRandomFile(allocatedHdfsPath, hfs, hdfsWorkPath);
        }
    }

    public Path createDistinctFile(String fileName, boolean local) throws IOException {
        if (local) {
            return createOneFile(fileName, allocatedLocalPath, lfs, localWorkPath);
        } else {
            return createOneFile(fileName, allocatedHdfsPath, hfs, hdfsWorkPath);
        }
    }

    private static synchronized Path createOneFile(String fileName, HashSet<Path> validation, FileSystem fs,
            Path workPath) throws IOException {
        Path path = new Path(workPath, fileName);
        if (fs.exists(path)) {
            throw new IllegalArgumentException("Given file exsited:" + fileName);
        }
        FSDataOutputStream os = fs.create(path, (short) 1);
        os.close();
        validation.add(path);
        return path;
    }

    private static synchronized Path createOneRandomFile(HashSet<Path> validation, FileSystem fs, Path workPath)
            throws IOException {
        Path path;
        do {
            path = new Path(workPath, "FileManager" + UUID.randomUUID());
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

    private static OutputStream getOutputStream(Path path, HashSet<Path> validation, FileSystem fs) throws IOException {
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