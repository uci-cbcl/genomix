package edu.uci.ics.genomix.pregelix.sequencefile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerCountValue;

public class GenerateSmallFile {

    public static void generateNumOfLinesFromGraphBuildResuiltBigFile(Path inFile, Path outFile, int numOfLines)
            throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSys = FileSystem.get(conf);

        SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, inFile, conf);
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf, outFile, KmerBytesWritable.class,
                KmerCountValue.class, CompressionType.NONE);
        KmerBytesWritable outKey = new KmerBytesWritable(55);
        KmerCountValue outValue = new KmerCountValue();
        int i = 0;

        for (i = 0; i < numOfLines; i++) {
            // System.out.println(i);
            reader.next(outKey, outValue);
            writer.append(outKey, outValue);
        }
        writer.close();
        reader.close();
    }

    public static void generateNumOfLinesFromGraphBuildResuiltBigFile(String inFile, String outFile, int numOfLines)
            throws IOException {
        String lines = readTextFile(inFile, numOfLines);
        writeTextFile(outFile, lines);
    }

    public static void main(String[] args) throws IOException {
        /*Path dir = new Path("data/test8m");
        Path outDir = new Path("data/input/test");
        FileUtils.cleanDirectory(new File("data/input/test"));
        Path inFile = new Path(dir, "part-0");
        Path outFile = new Path(outDir, "part-0-out-100");
        generateNumOfLinesFromGraphBuildResuiltBigFile(inFile, outFile, 100);*/
        String inFile = "data/shortjump_1.head8M.fastq";
        String outFile = "data/testGeneFile";
        generateNumOfLinesFromGraphBuildResuiltBigFile(inFile, outFile, 100000);
    }

    public static String readTextFile(String fileName, int numOfLines) {
        String returnValue = "";
        FileReader file;
        String line = "";
        try {
            file = new FileReader(fileName);
            BufferedReader reader = new BufferedReader(file);
            try {
                while ((numOfLines > 0) && (line = reader.readLine()) != null) {
                    returnValue += line + "\n";
                    numOfLines--;
                }
            } finally {
                reader.close();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found");
        } catch (IOException e) {
            throw new RuntimeException("IO Error occured");
        }
        return returnValue;

    }

    public static void writeTextFile(String fileName, String s) {
        FileWriter output;
        try {
            output = new FileWriter(fileName);
            BufferedWriter writer = new BufferedWriter(output);
            writer.write(s);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
