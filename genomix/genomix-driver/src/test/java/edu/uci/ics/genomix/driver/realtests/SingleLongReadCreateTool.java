package edu.uci.ics.genomix.driver.realtests;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.driver.GenomixDriver;
import edu.uci.ics.genomix.hyracks.graph.driver.GenomixHyracksDriver;

public class SingleLongReadCreateTool {
    /**
     * it is better to set the kmerSize a big value in case of duplicates, 
     * the target path which contain this string will be generated automatically
     * target path: relative path: longreadfortest
     */
    private static final char[] symbols = new char[4];
    private static final Logger LOG = Logger.getLogger(GenomixHyracksDriver.class.getName());
    static {
        symbols[0] = 'A';
        symbols[1] = 'C';
        symbols[2] = 'G';
        symbols[3] = 'T';
    }
    
    public enum KmerDir {
        FORWARD,
        REVERSE,
    }

    private final Random random = new Random();
    private char[] buf;
    private HashSet<Kmer> nonDupSet;
    private int k;
    private Kmer tempKmer;
    private Kmer tempReverseKmer;
    private KmerDir curKmerDir = KmerDir.FORWARD;
    private String targetPath;
    
    public SingleLongReadCreateTool(int kmerSize, int length) {
        if (length < 1)
            throw new IllegalArgumentException("length < 1: " + length);
        buf = new char[length];
        this.k = kmerSize;
        this.nonDupSet = new HashSet<Kmer>(length);
        Kmer.setGlobalKmerLength(kmerSize);
        tempKmer = new Kmer();
        tempReverseKmer = new Kmer();
        targetPath = "longreadfortest" + File.separator + "singlelongread_1.fastq";
    }

    public void generateString() {
        String tmp = "";
        int count = 4;
        LOG.info("Begin to generate string !");
        for (int idx = 0; idx < buf.length;) {
            buf[idx] = symbols[random.nextInt(4)];
            if (idx >= k - 1) {
                tmp = new String(buf, idx - k + 1, k);
                tempKmer.setFromStringBytes(tmp.getBytes(), 0);
                tempReverseKmer.setReversedFromStringBytes(tmp.getBytes(), 0);
                curKmerDir = tempKmer.compareTo(tempReverseKmer) <= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
                switch (curKmerDir.toString()){
                    case "FORWARD":
                        if (!nonDupSet.contains(tempKmer)) {
                            nonDupSet.add(new Kmer(tempKmer));
                            idx++;
                            count = 4;
                        } else if (count == 0) {
                            idx++;
                            count = 4;
                            LOG.info("there must be a duplicate in read! " + idx);
                        } else
                            count--;
                        break;
                    case "REVERSE":
                        if (!nonDupSet.contains(tempReverseKmer)) {
                            nonDupSet.add(new Kmer(tempReverseKmer));
                            idx++;
                            count = 4;
                        } else if (count == 0) {
                            idx++;
                            count = 4;
                        } else
                            count--;
                        break;
                }
            } else
                idx++;
        }
        LOG.info("End to generate string !");
    }
    
    public void writeToDisk() throws IOException {
        String targetStr = new String(buf);
        FileUtils.writeStringToFile(new File(targetPath), targetStr);
    }
    
    public void cleanDiskFile() throws IOException, InterruptedException {
        File targetFile = new File(targetPath);
        if(targetFile.getParentFile().exists()){
        String cleanFile = "rm -r -f " + targetFile.getParent();
        Process p = Runtime.getRuntime().exec(cleanFile);
        p.waitFor();
        if (p.exitValue() != 0)
            throw new RuntimeException("Failed to delete the path" + targetFile.getParent());
        }
    }
    
    public String getTestDir() {
        return targetPath;
    }
}
