package edu.uci.ics.genomix.pregelix.sequencefile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.VKmer;

public class GenerateTextFile {

    public static void convertGraphCleanOutputToText(String strSrcDir, String outPutDir) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSys = FileSystem.getLocal(conf);

        fileSys.create(new Path(outPutDir));
        BufferedWriter bw = new BufferedWriter(new FileWriter(outPutDir));
        File srcPath = new File(strSrcDir);
        for (File f : srcPath.listFiles((FilenameFilter) (new WildcardFileFilter("part*")))) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, new Path(f.getAbsolutePath()), conf);
            VKmer key = new VKmer();
            VertexValueWritable value = new VertexValueWritable();

            while (reader.next(key, value)) {
                if (key == null) {
                    break;
                }
                bw.write(key.toString() + "\t" + value.toString());
//                System.out.println(key.toString());
                bw.newLine();
            }
            reader.close();
        }
        bw.close();
    }

    public static void generateSpecificLengthChainFromNaivePathmergeResult(int maxLength) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter("naive_text_" + maxLength));
        Configuration conf = new Configuration();
        FileSystem fileSys = FileSystem.get(conf);
        for (int i = 0; i < 2; i++) {
            Path path = new Path("/home/anbangx/genomix_result/final_naive/part-" + i);
            SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, path, conf);
            VKmer key = new VKmer();
            VertexValueWritable value = new VertexValueWritable();

            while (reader.next(key, value)) {
                if (key == null || value == null) {
                    break;
                }
                if (value.getKmerLength() != -1 && value.getKmerLength() <= maxLength) {
                    bw.write(value.toString());
                    bw.newLine();
                }
            }
            reader.close();
        }
        bw.close();
    }

    public static void generateSpecificLengthChainFromLogPathmergeResult(int maxLength) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter("log_text_" + maxLength));
        Configuration conf = new Configuration();
        FileSystem fileSys = FileSystem.get(conf);
        for (int i = 0; i < 2; i++) {
            Path path = new Path("/home/anbangx/genomix_result/improvelog2/part-" + i);
            SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, path, conf);
            VKmer key = new VKmer();
            VertexValueWritable value = new VertexValueWritable();

            while (reader.next(key, value)) {
                if (key == null || value == null) {
                    break;
                }
                if (value.getKmerLength() != -1 && value.getKmerLength() <= maxLength) {
                    bw.write(key.toString() + "\t" + value.toString());
                    bw.newLine();
                }
            }
            reader.close();
        }
        bw.close();
    }

    public static void generateFromGraphbuildResult() throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter("textfile"));
        Configuration conf = new Configuration();
        FileSystem fileSys = FileSystem.get(conf);
        Path path = new Path("data/input/part-0-out-3000000");
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, path, conf);
        VKmer key = new VKmer();

        while (reader.next(key, null)) {
            if (key == null) {
                break;
            }
            bw.write(key.toString());
            bw.newLine();
        }
        reader.close();
        bw.close();
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        //generateFromPathmergeResult();
        //generateFromGraphbuildResult();
        //generateSpecificLengthChainFromPathmergeResult(68);
        //generateSpecificLengthChainFromLogPathmergeResult(68);
    }

}
