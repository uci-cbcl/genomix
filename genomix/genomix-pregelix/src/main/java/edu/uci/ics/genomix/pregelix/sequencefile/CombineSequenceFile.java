package edu.uci.ics.genomix.pregelix.sequencefile;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerCountValue;

public class CombineSequenceFile {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int kmerSize = 5;
        Configuration conf = new Configuration();
        FileSystem fileSys = FileSystem.get(conf);

        Path p = new Path("graphbuildresult/CyclePath2_result");
        //Path p2 = new Path("data/result");
        Path outFile = new Path("here");
        SequenceFile.Reader reader;
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf, outFile, KmerBytesWritable.class,
                KmerCountValue.class, CompressionType.NONE);
        KmerBytesWritable key = new KmerBytesWritable(kmerSize);
        KmerCountValue value = new KmerCountValue();

        File dir = new File("graphbuildresult/CyclePath2_result");
        for (File child : dir.listFiles()) {
            String name = child.getAbsolutePath();
            Path inFile = new Path(p, name);
            reader = new SequenceFile.Reader(fileSys, inFile, conf);
            while (reader.next(key, value)) {
                System.out.println(key.toString() + "\t" + value.toString());
                writer.append(key, value);
            }
            reader.close();
        }
        writer.close();
        System.out.println();

        reader = new SequenceFile.Reader(fileSys, outFile, conf);
        while (reader.next(key, value)) {
            System.err.println(key.toString() + "\t" + value.toString());
        }
        reader.close();
    }

}
